/*******************************************************************************
 * Copyright (c) 2007, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

/** @file
 * MQTT Protocol module - those functions needed by the core broker.
 * Other MQTT protocol related functions are in the MQTTProtocolOut and MQTTProtocolClient modules
 */

#include "MQTTProtocol.h"
#include "MQTTProtocolClient.h"
#include "Log.h"
#include "Topics.h"
#include "Clients.h"
#include "Bridge.h"
#include "Messages.h"
#include "Protocol.h"
#include "Users.h"
#include "StackTrace.h"


#if defined(MQTTS)
#include "MQTTSProtocol.h"
#endif


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

#include "Heap.h"

void MQTTProtocol_removePublication(Publications* p);

/**
 * function signature used in the handle_packets table
 */
typedef int (*pf)(void*, int, Clients*);

#if defined(NO_BRIDGE)
/**
 * table of function pointers for handling incoming packets without the bridge
 */
static pf handle_packets[] =
{
	NULL, /* RESERVED */
	MQTTProtocol_handleConnects,
	NULL, /* connack */
	MQTTProtocol_handlePublishes,
	MQTTProtocol_handlePubacks,
	MQTTProtocol_handlePubrecs,
	MQTTProtocol_handlePubrels,
	MQTTProtocol_handlePubcomps,
	MQTTProtocol_handleSubscribes,
	NULL, /* suback */
	MQTTProtocol_handleUnsubscribes,
	NULL, /* unsuback */
	MQTTProtocol_handlePingreqs,
	NULL, /* pingresp */
	MQTTProtocol_handleDisconnects
};
#else
/**
 * table of function pointers for handling incoming packets
 */
static pf handle_packets[] =
{
	NULL, /* RESERVED */
	MQTTProtocol_handleConnects,
	Bridge_handleConnacks,
	MQTTProtocol_handlePublishes,
	MQTTProtocol_handlePubacks,
	MQTTProtocol_handlePubrecs,
	MQTTProtocol_handlePubrels,
	MQTTProtocol_handlePubcomps,
	MQTTProtocol_handleSubscribes,
	MQTTProtocol_handleSubacks,
	MQTTProtocol_handleUnsubscribes,
	MQTTProtocol_handleUnsubacks,
	MQTTProtocol_handlePingreqs,
	MQTTProtocol_handlePingresps,
	MQTTProtocol_handleDisconnects
};
#endif

MQTTProtocol state;		/**< MQTT protocol state shared with the other MQTTProtocol modules */
BrokerStates* bstate;	/**< broker state shared with the other MQTTProtocol modules */
static time_t last_keepalive;	/**< time of last keep alive processing */
static int restarts = -1;	/**< number of MQTT protocol module restarts */

/**
 *  This flag indicates when we are reading, or trying to read, a packet from its socket.
 *  During this call we mustn't cleanup the client structure in MQTTProtocol_closesession
 *  even if we get a socket error as we still rely on that structure in MQTTProtocol_timeslice.
 *  MQTTProtocol_timeslice will cleanup the client structure instead.
 */
int in_MQTTPacket_Factory = -1;


MQTTProtocol* MQTTProtocol_getState()
{
	return &state;
}

/**
 * Reinitialize the MQTT protocol modules - has not been needed for a while.
 */
int MQTTProtocol_reinitialize()
{
	int rc = 0;

	FUNC_ENTRY;
	time(&(last_keepalive));
	time(&(bstate->start_time));
	bstate->last_autosave = bstate->start_time;
	++restarts;
#if defined(SINGLE_LISTENER)
	Log(LOG_INFO, 14, NULL, bstate->port);
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Initialize the MQTT protocol modules.
 * @param aBrokerState pointer to the broker state structure
 */
int MQTTProtocol_initialize(BrokerStates* aBrokerState)
{
	int rc = 0;
	FUNC_ENTRY;

	bstate = aBrokerState;
	memset(&state, '\0', sizeof(state));
	rc = MQTTProtocol_reinitialize();
	FUNC_EXIT_RC(rc);
	return rc;
}


void MQTTProtocol_shutdownclients(Tree* clients, int terminate)
{
	Node* current = NULL;

	FUNC_ENTRY;
	current = TreeNextElement(clients, current);
	while (current)
	{
		Clients* client = (Clients*)(current->content);
		current = TreeNextElement(clients, current);
		Log(LOG_INFO, 17, NULL, client->clientID);
		if (terminate)
			client->cleansession = 1; /* no persistence, so everything is clean */
		MQTTProtocol_closeSession(client, 0);
	}
	FUNC_EXIT;
}


/**
 * Shutdown the MQTT protocol modules.
 * @param terminate boolean flag to indicate whether the clients should all be closed
 */
void MQTTProtocol_shutdown(int terminate)
{
	FUNC_ENTRY;
	Log(LOG_INFO, 16, NULL);
	MQTTProtocol_shutdownclients(bstate->clients, terminate);
	MQTTProtocol_shutdownclients(bstate->disconnected_clients, terminate);
	FUNC_EXIT;
}


/**
 * Shutdown the MQTT protocol modules and close all clients.
 */
void MQTTProtocol_terminate()
{
	FUNC_ENTRY;
	MQTTProtocol_shutdown(1);
	FUNC_EXIT;
}


/**
 * Internal publication to a system topic.
 * @param topic the topic to publish on
 * @param string the data to publish
 */
void MQTTProtocol_sys_publish(char* topic, char* string)
{
	List* rpl = NULL;
	int doPublish = 1;

	FUNC_ENTRY;
	rpl = SubscriptionEngines_getRetained(bstate->se, topic);
	if (rpl->count > 0)  /* this should be at most 1, as we must use a non-wildcard topic (for publishing) */
	{
		RetainedPublications* rp = (RetainedPublications*)(rpl->first->content);
		if (strlen(string) == rp->payloadlen && memcmp(rp->payload, string, rp->payloadlen) == 0)
			doPublish = 0;
	}
	ListFreeNoContent(rpl);
	if (doPublish)
	{
		Publish publish;
		publish.header.byte = 0;
		publish.header.bits.retain = 1;
		publish.payload = string;
		publish.payloadlen = strlen(string);
		publish.topic = topic;
		MQTTProtocol_handlePublishes(&publish, 0, NULL);
	}
	FUNC_EXIT;
}


/**
 * Update the MQTT protocol statistics on the $SYS topics.
 */
void MQTTProtocol_update(time_t now)
{
	static char buf[30];
	static time_t last = 0;
	static int last_received = 0;
	static int last_sent = 0;
	static long unsigned int last_bytes_received = 0;
	static long unsigned int last_bytes_sent = 0;
	int i;
	socket_stats* ss = Socket_getStats();

	FUNC_ENTRY;
	sprintf(buf, "%d", (ss->more_work_count * 100) / (ss->more_work_count + ss->not_more_work_count));
	MQTTProtocol_sys_publish("$SYS/broker/internal/more_work%", buf);

	sprintf(buf, "%d", (ss->not_more_work_count * 100) / (ss->more_work_count + ss->not_more_work_count));
	MQTTProtocol_sys_publish("$SYS/broker/internal/not_more_work%", buf);

	sprintf(buf, "%d", (ss->timeout_zero_count * 100) / (ss->timeout_zero_count + ss->timeout_non_zero_count));
	MQTTProtocol_sys_publish("$SYS/broker/internal/timeout_zero%", buf);

	sprintf(buf, "%d", (ss->timeout_non_zero_count * 100) / (ss->timeout_zero_count + ss->timeout_non_zero_count));
	MQTTProtocol_sys_publish("$SYS/broker/internal/timeout_non_zero%", buf);

	sprintf(buf, "%d", bstate->msgs_sent);
	MQTTProtocol_sys_publish("$SYS/broker/messages/sent", buf);
	i = bstate->msgs_sent - last_sent;
	sprintf(buf, "%ld", i < 1 ? 0 : i /(now - last));
	MQTTProtocol_sys_publish("$SYS/broker/messages/per second/sent", buf);
	last_sent = bstate->msgs_sent;

	sprintf(buf, "%d", bstate->msgs_received);
	MQTTProtocol_sys_publish("$SYS/broker/messages/received", buf);
	i = bstate->msgs_received - last_received;
	sprintf(buf, "%ld", i < 1 ? 0 : i /(now - last));
	MQTTProtocol_sys_publish("$SYS/broker/messages/per second/received", buf);
	last_received = bstate->msgs_received;

	sprintf(buf, "%ld", bstate->bytes_sent);
	MQTTProtocol_sys_publish("$SYS/broker/bytes/sent", buf);
	i = bstate->bytes_sent - last_bytes_sent;
	sprintf(buf, "%ld", i < 1 ? 0 : i /(now - last));
	MQTTProtocol_sys_publish("$SYS/broker/bytes/per second/sent", buf);
	last_bytes_sent = bstate->bytes_sent;

	sprintf(buf, "%ld", bstate->bytes_received);
	MQTTProtocol_sys_publish("$SYS/broker/bytes/received", buf);
	i = bstate->bytes_received - last_bytes_received;
	sprintf(buf, "%ld", i < 1 ? 0 : i /(now - last));
	MQTTProtocol_sys_publish("$SYS/broker/bytes/per second/received", buf);
	last_bytes_received = bstate->bytes_received;

	sprintf(buf, "%d bytes", Heap_get_info()->current_size);
	MQTTProtocol_sys_publish("$SYS/broker/heap/current size", buf);
	sprintf(buf, "%d bytes", Heap_get_info()->max_size);
	MQTTProtocol_sys_publish("$SYS/broker/heap/maximum size", buf);
	sprintf(buf, "%d seconds", (int)difftime(now, bstate->start_time));
	MQTTProtocol_sys_publish("$SYS/broker/uptime", buf);
	sprintf(buf, "%d", restarts);
	MQTTProtocol_sys_publish("$SYS/broker/restart count", buf);

	sprintf(buf, "%d", bstate->clients->count);
	MQTTProtocol_sys_publish("$SYS/broker/client count/connected", buf);
	
	sprintf(buf, "%d", bstate->disconnected_clients->count);
	MQTTProtocol_sys_publish("$SYS/broker/client count/disconnected", buf);
	
	sprintf(buf, "%d", bstate->se->subs->count);
	MQTTProtocol_sys_publish("$SYS/broker/subscriptions/count", buf);
	
	sprintf(buf, "%d", bstate->se->wsubs->count);
	MQTTProtocol_sys_publish("$SYS/broker/wildcard_subscriptions/count", buf);

	sprintf(buf, "%d", bstate->se->retaineds->count);
	MQTTProtocol_sys_publish("$SYS/broker/retained messages/count", buf);

	sprintf(buf, "%d", bstate->max_queued_messages);
	MQTTProtocol_sys_publish("$SYS/broker/settings/max_queued_messages", buf);

	sprintf(buf, "%d", bstate->max_inflight_messages);
	MQTTProtocol_sys_publish("$SYS/broker/settings/max_inflight_messages", buf);
	
	sprintf(buf, "%d", bstate->ffdc_count);
	MQTTProtocol_sys_publish("$SYS/broker/ffdc/count", buf);

	if (bstate->persistence == 1)
	{
		if (bstate->autosave_on_changes == 0 && bstate->autosave_interval > 0
			&& bstate->se->retained_changes > 0 && (int)difftime(now, bstate->last_autosave) > bstate->autosave_interval)
		{
			Log(LOG_INFO,  101, NULL, bstate->autosave_interval);
			SubscriptionEngines_save(bstate->se);
			bstate->last_autosave = now;
		}
		if (bstate->hup_signal)
		{
			if (bstate->se->retained_changes > 0)
			{
				Log(LOG_INFO, 104, NULL);
				SubscriptionEngines_save(bstate->se);
			}
			else
				Log(LOG_INFO, 105, NULL);
			bstate->hup_signal = 0;
		}
	}
	last = now;
	FUNC_EXIT;
}


/**
 * See if any pending writes have been completed, and cleanup if so.
 * Cleaning up means removing any publication data that was stored because the write did
 * not originally complete.
 */
void MQTTProtocol_checkPendingWrites()
{
	FUNC_ENTRY;
	if (state.pending_writes.count > 0)
	{
		ListElement* le = state.pending_writes.first;
		while (le)
		{
			pending_write* pw = (pending_write*)(le->content);
			if (Socket_noPendingWrites(pw->socket))
			{
				Clients* client = pw->client;

				MQTTProtocol_removePublication(pw->p);
				state.pending_writes.current = le;
				ListRemove(&(state.pending_writes), le->content); /* does NextElement itself */
				le = state.pending_writes.current;
				/* now we might be able to write the next message in the queue */
				MQTTProtocol_processQueued(client);
			}
			else
				ListNextElement(&(state.pending_writes), &le);
		}
	}
	FUNC_EXIT;
}


/**
 * MQTT protocol keepalive and retry processing.
 */
int MQTTProtocol_housekeeping(int more_work)
{
	time_t now = 0;

	FUNC_ENTRY;
	time(&(now));
	if (difftime(now, last_keepalive) > 5)
	{
		time(&(last_keepalive));
		MQTTProtocol_keepalive(now);
		more_work = MQTTProtocol_retry(now, 1);
		MQTTProtocol_update(now);
		Socket_cleanNew(now);
	}
	/*else
		more_work = MQTTProtocol_retry(now, 0);*/
	FUNC_EXIT_RC(more_work);
	return more_work;
}


/**
 * MQTT protocol timeslice for one packet and client - must not take too long!
 * @param sock the socket which is ready for the packet to be read from
 * @param client the client structure which corresponds to the socket
 */
void MQTTProtocol_timeslice(int sock, Clients* client)
{
	int error;
	MQTTPacket* pack;

	FUNC_ENTRY;

	Log(TRACE_MIN, -1, "%d %s About to read packet for peer address %s",
			sock, (client == NULL) ? "unknown" : client->clientID, Socket_getpeer(sock));
	in_MQTTPacket_Factory = sock;
	pack = MQTTPacket_Factory(sock, &error);
	in_MQTTPacket_Factory = -1;
	if (pack == NULL)
	{ /* there was an error on the socket, so clean it up */
		if (error == SOCKET_ERROR || error == BAD_MQTT_PACKET)
		{
			if (client != NULL)
			{
				client->good = 0; /* make sure we don't try and send messages to ourselves */
				if (error == SOCKET_ERROR)
					Log(LOG_WARNING, 18, NULL, client->clientID, sock, Socket_getpeer(sock));
				else
					Log(LOG_WARNING, 19, NULL, client->clientID, sock, Socket_getpeer(sock));
				MQTTProtocol_closeSession(client, 1);
			}
			else
			{
				if (error == SOCKET_ERROR)
					/* Don't do a Socket_getpeer in the case of SOCKET_ERROR -
					 * otherwise another SOCKET_ERROR will be hit  */
					Log(LOG_WARNING, 20, NULL, sock, "unknown");
				else
					Log(LOG_WARNING, 21, NULL, sock, Socket_getpeer(sock));
				Socket_close(sock);
			}
		}
	}
	else if (handle_packets[pack->header.bits.type] == NULL)
		Log(LOG_WARNING, 22, NULL, pack->header.bits.type, sock);
	else
	{
		if (client == NULL && pack->header.bits.type != CONNECT)
		{
			Log(LOG_WARNING, 23, NULL, sock, Socket_getpeer(sock), MQTTPacket_name(pack->header.bits.type));
			MQTTPacket_free_packet(pack);
			Socket_close(sock);
		}
		else
		{
			Node* elem = NULL;
			/* incoming publish at QoS 0 does not result in outgoing communication, so we don't want to count it as contact,
			   for PING processing on outbound connections */
			int update_time = (pack->header.bits.type == PUBLISH && pack->header.bits.qos == 0) ? 0 : 1;
			if (client && (update_time || (client->outbound == 0)))
				time(&(client->lastContact));
			if ((*handle_packets[pack->header.bits.type])(pack, sock, client) == SOCKET_ERROR)
			{
				/* the client could have been closed during handle_packet, so check to see if it's still in the client list */
				elem = TreeFind(bstate->clients, &sock);
				if (elem == NULL && client && client->clientID)
					elem = TreeFind(bstate->disconnected_clients, client->clientID);
				if (elem != NULL)
				{
					client = (Clients*)(elem->content);
					client->good = 0; /* make sure we don't try and send messages to ourselves */
					Log(LOG_WARNING, 18, NULL, client->clientID, sock, Socket_getpeer(sock));
					MQTTProtocol_closeSession(client, 1);
				}
				else
				{
					Log(LOG_WARNING, 20, NULL, sock, Socket_getpeer(sock));
					Socket_close(sock);
				}
			}
		}
	}
	/*MQTTProtocol_housekeeping(); move to Protocol_timeslice*/
	FUNC_EXIT;
}


/**
 * Clean up a client list by closing any marked as "not good".
 * @param clients the list of clients
 */
void MQTTProtocol_clean_clients(Tree* clients)
{
	Node* current = NULL;

	FUNC_ENTRY;
	current = TreeNextElement(clients, current);
	while (current)
	{
		Clients* client =	(Clients*)(current->content);
		current = TreeNextElement(clients, current);

		if (client->good == 0)
		{
			Log(LOG_WARNING, 18, NULL, client->clientID, client->socket,
					Socket_getpeer(client->socket));
			MQTTProtocol_closeSession(client, 1);
		}
	}
	FUNC_EXIT;
}


/**
 * Free up the will memory for a client structure.
 * @param client the client
 */
void MQTTProtocol_clearWill(Clients* client)
{
	FUNC_ENTRY;
	if (client->will != NULL)
	{
		free(client->will->msg);
		free(client->will->topic);
		free(client->will);
		client->will = NULL;
	}
	FUNC_EXIT;
}


#if defined(MQTTS)
void MQTTProtocol_setWillTopic(Clients* client, char* topic, int retained, int qos)
{
	FUNC_ENTRY;
	if (client->will != NULL)
	{
		if (client->will->topic != NULL)
			free(client->will->topic);
	}
	else
	{
		client->will = (willMessages*)malloc(sizeof(willMessages));
		client->will->msg = NULL;
	}
	client->will->topic = topic;
	client->will->retained = retained;
	client->will->qos = qos;
	FUNC_EXIT;
}


void MQTTProtocol_setWillMsg(Clients* client, char* msg)
{
	FUNC_ENTRY;
	if (client->will != NULL)
	{
		if (client->will->msg != NULL)
			free(client->will->msg);
	}
	else
	{
		client->will = (willMessages*)malloc(sizeof(willMessages));
		client->will->topic = NULL;
	}
	client->will->msg = msg;
	FUNC_EXIT;
}

#else
/**
 * Set the will parameters for a client according to a connect packet.
 * @param connect pointer to the connect packet structure
 * @param client the client
 */
void MQTTProtocol_setWill(Connect* connect, Clients* client)
{
	FUNC_ENTRY;
	if (connect->flags.bits.will)
	{
		if (client->will != NULL)
		{
			free(client->will->msg);
			free(client->will->topic);
		}
		else
			client->will = (willMessages*)malloc(sizeof(willMessages));
		client->will->msg = connect->willMsg;
		connect->willMsg = NULL; /* don't free this memory */
		client->will->topic = connect->willTopic;
		connect->willTopic = NULL; /* don't free this memory */
		client->will->retained = connect->flags.bits.willRetain;
		client->will->qos = connect->flags.bits.willQoS;
	}
	else
		MQTTProtocol_clearWill(client);
	FUNC_EXIT;
}
#endif


/**
 * Remove all subscriptions for a client.
 * @param clientID the client ID of the client to be unsubscribed
 */
void MQTTProtocol_removeAllSubscriptions(char* clientID)
{
	FUNC_ENTRY;
	SubscriptionEngines_unsubscribe(bstate->se, clientID, (char*)MULTI_LEVEL_WILDCARD);
	SubscriptionEngines_unsubscribe(bstate->se, clientID, "$SYS/#");
	FUNC_EXIT;
}


/**
 * Process an incoming connect packet for a socket
 * @param pack pointer to the connect packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleConnects(void* pack, int sock, Clients* client)
{
	Connect* connect = (Connect*)pack;
	Node* elem = NULL;
	int terminate = 0;
	int rc = TCPSOCKET_COMPLETE;
#if !defined(SINGLE_LISTENER)
	Listener* listener = Socket_getParentListener(sock);
#endif

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 26, NULL, sock, connect->clientID);/*
			connect->Protocol, connect->flags.bits.cleanstart, connect->keepAliveTimer,
			connect->version, connect->username, connect->password);*/
	Socket_removeNew(sock);		
			
	if (bstate->state != BROKER_RUNNING)
		terminate = 1; /* don't accept new connection requests when we are shutting down */
	/* Now check the version.  If we don't recognize it we will not have parsed the packet,
	 * so nothing else in the packet structure will have been filled in.
	 */
	else if (!MQTTPacket_checkVersion(pack))
	{
		Log(LOG_WARNING, 32, NULL, connect->Protocol, connect->version);
		rc = MQTTPacket_send_connack(CONNACK_UNACCEPTABLE_PROTOCOL_VERSION, sock, Socket_getpeer(sock)); /* send response */
		terminate = 1;
	}
	else if (connect->clientID[0] == '\0' || (connect->version == 3 && strlen(connect->clientID) > 23))
	{
		rc = MQTTPacket_send_connack(CONNACK_IDENTIFIER_REJECTED, sock, Socket_getpeer(sock)); /* send response */
		terminate = 1;
	}
	else if (bstate->password_file != NULL)
	{
		if (connect->flags.bits.username && connect->flags.bits.password &&
				(Users_authenticate(connect->username, connect->password) == false))
		{
			Log(LOG_WARNING, 31, NULL, connect->clientID);
			rc = MQTTPacket_send_connack(CONNACK_BAD_USERNAME_OR_PASSWORD, sock, connect->clientID); /* send bad user/pass response */
			terminate = 1;
		}
		else if ((!connect->flags.bits.username || !connect->flags.bits.password) && !bstate->allow_anonymous)
		{
			Log(LOG_WARNING, 31, NULL, connect->clientID);
			rc = MQTTPacket_send_connack(CONNACK_BROKER_UNAVAILABLE, sock, connect->clientID); /* send broker unavailable response */
			terminate = 1;
		}
	}

	if (terminate)
		;
	else if (bstate->clientid_prefixes->count > 0 &&
		!ListFindItem(bstate->clientid_prefixes, connect->clientID, clientPrefixCompare))
	{
		Log(LOG_WARNING, 31, NULL, connect->clientID);
		terminate = 1;
	}
	else
	{
#if !defined(SINGLE_LISTENER)
		if (listener->max_connections > -1 &&
				listener->connections->count > listener->max_connections)
		{
			Log(LOG_WARNING, 141, NULL, connect->clientID, listener->max_connections, listener->port);
#else
		if (bstate->max_connections > -1 &&
			MQTTProtocol_getNoConnectedClients() >= bstate->max_connections)
		{
			Log(LOG_WARNING, 141, NULL, connect->clientID, bstate->max_connections, bstate->port);
#endif
			rc = MQTTPacket_send_connack(CONNACK_BROKER_UNAVAILABLE, sock, connect->clientID); /* send response */
			terminate = 1;
		}
	}

	if (terminate)
	{
		MQTTPacket_freeConnect(connect);
		Socket_close(sock);
		rc = TCPSOCKET_COMPLETE;
		goto exit;
	}

	if (bstate->connection_messages)
		Log(LOG_INFO,
#if !defined(SINGLE_LISTENER)
			33, NULL, listener->port, connect->clientID, Socket_getpeer(sock));
#else
			33, NULL, bstate->port, connect->clientID, Socket_getpeer(sock));
#endif

	elem = TreeFindIndex(bstate->clients, connect->clientID, 1);
	if (elem == NULL)
	{
		client = TreeRemoveKey(bstate->disconnected_clients, connect->clientID);
		if (client == NULL)
		{
			int i;
			char* tmpAddr = NULL;

			client = malloc(sizeof(Clients));
			memset(client, '\0', sizeof(Clients));
			tmpAddr = Socket_getpeer(sock);
			client->addr = malloc(strlen(tmpAddr)+1);
			strcpy(client->addr, tmpAddr);
	#if defined(MQTTS)
			client->protocol = PROTOCOL_MQTT;
	#endif
			client->clientID = connect->clientID;
			client->outboundMsgs = ListInitialize();
			client->inboundMsgs = ListInitialize();
			for (i = 0; i < PRIORITY_MAX; ++i)
				client->queuedMsgs[i] = ListInitialize();
			connect->clientID = NULL; /* don't want to free this space as it is being used in the client structure */
		}
		client->socket = sock;
		TreeAdd(bstate->clients, client, sizeof(Clients) + strlen(client->clientID)+1 + 3*sizeof(List));
	}
	else
	{
		client = (Clients*)(elem->content);
		if (client->connected)
		{
			Log(LOG_INFO, 34, NULL, connect->clientID, Socket_getpeer(sock));
			if (client->socket != sock)
				Socket_close(client->socket);
		}

		if (connect->flags.bits.cleanstart)
		{
			int i;
			/* empty pending message lists */
			MQTTProtocol_emptyMessageList(client->outboundMsgs);
			MQTTProtocol_emptyMessageList(client->inboundMsgs);
			for (i = 0; i < PRIORITY_MAX; ++i)
				MQTTProtocol_emptyMessageList(client->queuedMsgs[i]);
			client->msgID = client->outbound = client->ping_outstanding = 0;
		}

		/* have to remove and re-add client so it is in the right order for new socket */
		if (client->socket != sock)
		{
			TreeRemoveNodeIndex(bstate->clients, elem, 1);
			TreeRemoveKeyIndex(bstate->clients, &client->socket, 0);
			client->socket = sock;
			TreeAdd(bstate->clients, client, sizeof(Clients) + strlen(client->clientID)+1 + 3*sizeof(List));
		}
	}

	client->good = client->connected = 1;
	client->cleansession = connect->flags.bits.cleanstart;
	client->keepAliveInterval = connect->keepAliveTimer;
	client->noLocal = (connect->version == PRIVATE_PROTOCOL_VERSION) ? 1 : 0;
	if (client->cleansession)
		MQTTProtocol_removeAllSubscriptions(client->clientID); /* clear any persistent subscriptions */
#if !defined(SINGLE_LISTENER)
	if (listener && listener->mount_point && connect->flags.bits.will)
	{
		char* temp = malloc(strlen(connect->willTopic) + strlen(listener->mount_point) + 1);
		strcpy(temp, listener->mount_point);
		strcat(temp, connect->willTopic);
		free(connect->willTopic);
		connect->willTopic = temp;
	}
#endif
#if defined(MQTTS)
	if (connect->flags.bits.will)
	{
		MQTTProtocol_setWillTopic(client, connect->willTopic,
									connect->flags.bits.willRetain, connect->flags.bits.willQoS);
		MQTTProtocol_setWillMsg(client, connect->willMsg);
		connect->willTopic = NULL;
		connect->willMsg = NULL;
	}
#else
	MQTTProtocol_setWill(connect, client);
#endif

	if (connect->flags.bits.username)
	{
		client->user = Users_get_user(connect->username);
	}

	rc = MQTTPacket_send_connack(CONNACK_CONNECTION_ACCEPTED, sock, client->clientID); /* send response */

	if (client->cleansession == 0)
	{
		ListElement* outcurrent = NULL;
		time_t now = 0;

		/* ensure that inflight messages are retried now by setting the last touched time
		 * to very old (0) before calling the retry function
		 */
		time(&(now));
		while (ListNextElement(client->outboundMsgs, &outcurrent))
		{
			Messages* m = (Messages*)(outcurrent->content);
			m->lastTouch = 0;
		}
		MQTTProtocol_retries(now, client);
		MQTTProtocol_processQueued(client);
	}
	time(&(client->lastContact));

	MQTTPacket_freeConnect(connect);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming ping request packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePingreqs(void* pack, int sock, Clients* client)
{
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 3, NULL, sock, client->clientID);
	rc = MQTTPacket_send_pingresp(sock, client->clientID);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming disconnect packet for a socket
 * @param pack pointer to the disconnect packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleDisconnects(void* pack, int sock, Clients* client)
{
	FUNC_ENTRY;
	client->good = 0; /* don't try and send log message to this client if it is subscribed to $SYS/broker/log */
	Log(LOG_PROTOCOL, 5, NULL, sock, client->clientID);
	if (bstate->connection_messages)
		Log(LOG_INFO, 38, NULL, client->clientID);
	MQTTProtocol_closeSession(client, 0);
	FUNC_EXIT;
	return TCPSOCKET_COMPLETE;
}


/**
 * Process retained messages (when a client subscribes)
 * @param client the client to send the messages to
 * @param topic the topic to match
 * @param qos the QoS of the subscription
 */
void MQTTProtocol_processRetaineds(Clients* client, char* topic, int qos, int priority)
{
	List* rpl = NULL;
	ListElement* currp = NULL;
#if defined(QOS0_SEND_LIMIT)
	int qos0count = 0;
#endif

	FUNC_ENTRY;
	rpl = SubscriptionEngines_getRetained(bstate->se, topic);
	while (ListNextElement(rpl, &currp))
	{
		int curqos;
		Publish publish;
		Messages* p = NULL;
		RetainedPublications* rp = (RetainedPublications*)(currp->content);

		publish.payload = rp->payload;
		publish.payloadlen = rp->payloadlen;
		publish.topic = rp->topicName;
		curqos = (rp->qos < qos) ? rp->qos : qos;
#if defined(QOS0_SEND_LIMIT)
		if (curqos == 0)
			++qos0count;
		if (qos0count > bstate->max_inflight_messages) /* a somewhat arbitrary criterion */
		{
			if (MQTTProtocol_queuePublish(client, &publish, curqos, 1, priority, &p) == SOCKET_ERROR)
				break;
		}
		else
		{
#endif
			if (Protocol_startOrQueuePublish(client, &publish, curqos, 1, priority, &p) == SOCKET_ERROR)
				break;
#if defined(QOS0_SEND_LIMIT)
		}
#endif
	}
	ListFreeNoContent(rpl);
	FUNC_EXIT;
}


/**
 * Process an incoming ping subscribe packet for a socket
 * @param pack pointer to the subscribe packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleSubscribes(void* pack, int sock, Clients* client)
{
	int i, *aq, *isnew, *authorized;
	Subscribe* subscribe = (Subscribe*)pack;
	ListElement *curtopic = NULL, *curqos = NULL;
	int rc = TCPSOCKET_COMPLETE;
#if !defined(SINGLE_LISTENER)
	Listener* listener = Socket_getParentListener(sock);
#endif

	FUNC_ENTRY;
	if (Protocol_isClientQuiescing(client))
		goto exit; /* don't accept new work */

	Log(LOG_PROTOCOL, 6, NULL, sock, client->clientID, subscribe->msgId);
	aq = malloc(sizeof(int)*(subscribe->noTopics));
	isnew = malloc(sizeof(int)*(subscribe->noTopics));
	authorized = malloc(sizeof(int)*(subscribe->noTopics));
	for (i = 0; i < subscribe->noTopics; ++i)
	{
		int j;
		ListElement *duptopic = NULL;

		ListNextElement(subscribe->topics, &curtopic);
		aq[i] = *(int*)(ListNextElement(subscribe->qoss, &curqos)->content);

		/* The mount_point topic transformation must be done before the topic syntax validity check
		 * otherwise badly formed topics can get into the subscription engine.
		 */
#if !defined(SINGLE_LISTENER)
		if (listener && listener->mount_point)
		{
			char* temp = malloc(strlen((char*)(curtopic->content)) + strlen(listener->mount_point) + 1);
			strcpy(temp, listener->mount_point);
			strcat(temp, (char*)(curtopic->content));
			free((char*)(curtopic->content));
			curtopic->content = temp;
			subscribe->topics->size += strlen(listener->mount_point);
		}
#endif

		if (!Topics_isValidName((char*)curtopic->content))
		{
			Log(LOG_WARNING, 153, NULL, (char*)curtopic->content, client->clientID, client->addr);
			free(curtopic->content);
			continue;
		}

		authorized[i] = true;
		if (bstate->password_file && bstate->acl_file)
		{
			authorized[i] = Users_authorise(client->user,(char*)(curtopic->content),ACL_READ);
			if (!authorized[i])
				Log(LOG_AUDIT, 150, NULL, client->clientID, (char*)(curtopic->content));
		}

		for (j = 0; j < i; ++j)
		{
			char* prevtopic = (char*)(ListNextElement(subscribe->topics, &duptopic)->content);
			if (strcmp(prevtopic, (char*)(curtopic->content)) == 0)
				duptopic->content = curtopic->content;
		}
		isnew[i] = SubscriptionEngines_subscribe(bstate->se, client->clientID,
			(char*)(curtopic->content), aq[i], client->noLocal, (client->cleansession == 0), PRIORITY_NORMAL);
	}
	/* send suback before sending the retained publications because a lot of retained publications could fill up the socket buffer */
	if ((rc = MQTTPacket_send_suback(subscribe->msgId, subscribe->noTopics, aq, sock, client->clientID)) != SOCKET_ERROR)
	{
		curtopic = curqos = NULL;
		for (i = 0; i < subscribe->noTopics; ++i)
		{
			/* careful if you get >1 subscriptions using the same topic name in the same packet! */
			/* The next line changes the semantics of subscribe for bridge connections,
			 * so that retained messages are only sent for new subscriptions.  This is to help
			 * avoid "retained message storms" when a connection drops and is re-established.
			 * This change could be applied to all subscriptions by removing the "noLocal" check. */
			if (authorized[i] && ((client->noLocal == 0) || isnew[i]))
				MQTTProtocol_processRetaineds(client, (char*)(ListNextElement(subscribe->topics, &curtopic)->content),
					*(int*)(ListNextElement(subscribe->qoss, &curqos)->content), PRIORITY_NORMAL);
		}
	}
	free(aq);
	free(isnew);
	free(authorized);
exit:
	MQTTPacket_freeSubscribe(subscribe, 0);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming unsubscribe packet for a socket
 * @param pack pointer to the unsubscribe packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleUnsubscribes(void* pack, int sock, Clients* client)
{
	int i;
	ListElement* curtopic = NULL;
	Unsubscribe* unsubscribe = (Unsubscribe*)pack;
	int rc = TCPSOCKET_COMPLETE;
#if !defined(SINGLE_LISTENER)
	Listener* listener = Socket_getParentListener(sock);
#endif

	FUNC_ENTRY;
	if (Protocol_isClientQuiescing(client))
		goto exit; /* don't accept new work */

	Log(LOG_PROTOCOL, 8, NULL, sock, client->clientID, unsubscribe->msgId);
	for (i = 0; i < unsubscribe->noTopics; ++i)
	{
		ListNextElement(unsubscribe->topics, &curtopic);
#if !defined(SINGLE_LISTENER)
		if (listener && listener->mount_point)
		{
			char* temp = malloc(strlen((char*)(curtopic->content)) + strlen(listener->mount_point) + 1);
			strcpy(temp, listener->mount_point);
			strcat(temp, (char*)(curtopic->content));
			free((char*)(curtopic->content));
			curtopic->content = temp;
			unsubscribe->topics->size += strlen(listener->mount_point);
		}
#endif
		SubscriptionEngines_unsubscribe(bstate->se, client->clientID, (char*)(curtopic->content));
	}
	rc = MQTTPacket_send_unsuback(unsubscribe->msgId, sock, client->clientID); /* send response */
exit:
	MQTTPacket_freeUnsubscribe(unsubscribe);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Close any active session for a client and clean up.
 * @param client the client to clean up
 * @param send_will flag to indicate whether a will messsage should be sent if it has been set
 */
void MQTTProtocol_closeSession(Clients* client, int send_will)
{
	FUNC_ENTRY;
	client->good = 0;
	if (in_MQTTPacket_Factory == client->socket || client->closing)
		goto exit;
	client->closing = 1;
	if (client->socket > 0)
	{
		if (client->connected)
		{
			if (client->outbound && client->will)
			{
				Publish pub;
				pub.payload = "0";
				pub.payloadlen = 1;
				pub.topic = client->will->topic;
#if defined(MQTTS)
				if (client->protocol == PROTOCOL_MQTTS)
					MQTTSProtocol_startPublishCommon(client, &pub, 0,0,1);
				else
#endif
					MQTTPacket_send_publish(&pub, 0, 0, 1, client->socket, client->clientID);
				MQTTProtocol_sys_publish(client->will->topic, "0");
			}
#if defined(MQTTS)
			if (client->protocol == PROTOCOL_MQTTS_MULTICAST)
				;
			else if (client->protocol == PROTOCOL_MQTTS)
				MQTTSPacket_send_disconnect(client, 0);
			else
#endif
			if (client->outbound)
				MQTTPacket_send_disconnect(client->socket, client->clientID);
		}

		if (ListFindItem(&(state.pending_writes), &(client->socket), intcompare))
		{
			pending_write* pw = (pending_write*)(state.pending_writes.current->content);
			MQTTProtocol_removePublication(pw->p);
			ListRemove(&(state.pending_writes), pw);
		}

#if defined(MQTTS)
		if (client->protocol == PROTOCOL_MQTT || client->outbound == 1)
		{
			if (client->protocol == PROTOCOL_MQTTS_MULTICAST)
				Socket_close_only(client->socket);
			else
#endif
				Socket_close(client->socket);
#if defined(MQTTS)
		}
#endif
	}
	if (client->connected || client->connect_state)
	{
		client->connected = 0;
		client->connect_state = 0;
	}
	if (client->outbound == 0 && client->will != NULL && send_will)
	{
		Publish publish;
		publish.payload = client->will->msg;
		publish.payloadlen = strlen(client->will->msg);
		publish.topic = client->will->topic;
		publish.header.bits.qos = client->will->qos;
		publish.header.bits.retain = client->will->retained;
		Protocol_processPublication(&publish, client->clientID);
	}
#if defined(MQTTS)
	if (client->protocol == PROTOCOL_MQTTS)
		MQTTSProtocol_emptyRegistrationList(client->registrations);
#endif
	if (client->cleansession)
	{
		if (client->outbound && ((BridgeConnections*)(client->bridge_context))->state != CONNECTION_DELETE)
		{ /* bridge outbound client structures are reused on reconnection */
			int i;
			MQTTProtocol_removeAllSubscriptions(client->clientID);
			MQTTProtocol_emptyMessageList(client->inboundMsgs);
			MQTTProtocol_emptyMessageList(client->outboundMsgs);
			for (i = 0; i < PRIORITY_MAX; ++i)
				MQTTProtocol_emptyMessageList(client->queuedMsgs[i]);
			client->msgID = 0;
		}
		else
		{
#if defined(MQTTS)
			if ((client->protocol == PROTOCOL_MQTTS && client->outbound == 0) || client->protocol == PROTOCOL_MQTTS_MULTICAST)
			{
				if (!TreeRemove(bstate->mqtts_clients, client) && !TreeRemove(bstate->disconnected_mqtts_clients, client))
					Log(LOG_ERROR, 39, NULL);
				else
					Log(TRACE_MAX, 2, NULL, client->clientID, client->socket);
			}
			else
			{
#endif
				if (!TreeRemove(bstate->clients, client) && !TreeRemove(bstate->disconnected_clients, client))
					Log(LOG_ERROR, 39, NULL);
				else
					Log(TRACE_MAX, 2, NULL, client->clientID, client->socket);
#if defined(MQTTS)
			}
#endif
			MQTTProtocol_freeClient(client);
		}
	}
	else
	{
		int i;
		for (i = 0; i < PRIORITY_MAX; ++i)
			MQTTProtocol_removeQoS0Messages(client->queuedMsgs[i]);
#if defined(MQTTS)
		if (client->protocol == PROTOCOL_MQTTS && client->outbound == 0)
		{
			if (TreeRemove(bstate->mqtts_clients, client))
			{
				client->socket = 0;
				TreeAdd(bstate->disconnected_mqtts_clients, client, sizeof(Clients) + strlen(client->clientID)+1 + 3*sizeof(List));
			}
		}
		else
		{
#endif
		if (TreeRemove(bstate->clients, client))
		{
			client->socket = 0;
			TreeAdd(bstate->disconnected_clients, client, sizeof(Clients) + strlen(client->clientID)+1 + 3*sizeof(List));
		}
#if defined(MQTTS)
		}
#endif
    	client->closing = 0;
	}

exit:
	FUNC_EXIT;
}





