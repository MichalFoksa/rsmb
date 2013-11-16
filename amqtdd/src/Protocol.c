/*******************************************************************************
 * Copyright (c) 2009, 2013 IBM Corp.
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

/**
 * @file
 * General client protocol handling module.
 * Protocol-specific functions (MQTT, MQTTs) are in other modules.
 */

#include "Socket.h"
#include "Log.h"
#include "Messages.h"
#include "Protocol.h"
#include "MQTTPacket.h"
#include "MQTTProtocol.h"
#include "Topics.h"
#include "Persistence.h"
#include "StackTrace.h"

#include "Heap.h"

#if defined(MQTTS)
#include "MQTTSProtocol.h"
#endif
#if defined(MQTTMP)
#include "MQTTMPProtocol.h"
#endif

#include <string.h>

/**
 * Broker state structure.
 */
BrokerStates* bstate;

/**
 * Initializes the protocol module
 * @param bs pointer to a broker state structure
 */
int Protocol_initialize(BrokerStates* bs)
{
	int rc = 0;

	FUNC_ENTRY;
	bstate = bs;
	rc = MQTTProtocol_initialize(bs);
#if defined(MQTTS)
	rc = MQTTSProtocol_initialize(bs);
#endif
#if defined(MQTTMP)
	MQTTMPProtocol_initialize(bs);
#endif
	FUNC_EXIT;
	return rc;
}


/**
 * Cleans up the protocol module
 */
void Protocol_terminate()
{
	FUNC_ENTRY;
	MQTTProtocol_terminate();
#if defined(MQTTS)
	//MQTTSProtocol_terminate();
#endif
#if defined(MQTTMP)
	MQTTMPProtocol_terminate();
#endif
	FUNC_EXIT;
}


/**
 * Indicates whether a client has any in-progress message exchanges.
 * Used to enable a clean shutdown of clients, and the broker.
 * @param client pointer to a client structure
 * @return boolean - are there any messages still
 */
int Protocol_inProcess(Clients* client)
{
	return client->inboundMsgs->count > 0 || client->outboundMsgs->count > 0;
}


/**
 * Close clients during a clean shutdown
 */
void Protocol_closing()
{
	int connected_count = 0;
	ListElement* current = NULL;

	FUNC_ENTRY;
	ListNextElement(bstate->clients, &current);
	while (current)
	{
		Clients* client =	(Clients*)(current->content);
		ListNextElement(bstate->clients, &current);

		if (client->connected)
		{
			/* we can close a client if there are no in-process messages */
			if (!Protocol_inProcess(client))
			{
#if defined(MQTTMP)
				if (client->protocol == PROTOCOL_MQTT_MP)
					MQTTMPProtocol_closeSession(client,0);
#endif
				MQTTProtocol_closeSession(client, 0);
			}
			else
				++connected_count;
		}
	}

	if (connected_count == 0)
		bstate->state = BROKER_STOPPED;
	FUNC_EXIT;
}


/**
 * Timeslice function to run protocol exchanges
 */
void Protocol_timeslice()
{
	int sock;
	int bridge_connection = 0;
	static int more_work = 0;

	FUNC_ENTRY;
	if ((sock = Socket_getReadySocket(more_work, NULL)) == SOCKET_ERROR)
	{
#if defined(WIN32)
		int errno;
		errno = WSAGetLastError();
#endif
		if (errno != EINTR && errno != EAGAIN && errno != EINPROGRESS && errno != EWOULDBLOCK)
		{
			Log(LOG_SEVERE, 0, "Restarting MQTT protocol to resolve socket problems");
			MQTTProtocol_shutdown(0);
			SubscriptionEngines_save(bstate->se);
			MQTTProtocol_reinitialize();
			goto exit;
		}
	}

	MQTTProtocol_checkPendingWrites();
	if (sock > 0)
	{
		Clients* client = NULL;

		if (ListFindItem(bstate->clients, &sock, clientSocketCompare) != NULL)
		{
			client = (Clients*)(bstate->clients->current->content);
#if !defined(NO_BRIDGE)
			if (client->outbound && client->connect_state == 1)
			{
				Bridge_handleConnection(client);
				bridge_connection = 1;
			}
#endif
		}
		if (bridge_connection == 0)
#if defined(MQTTS) || defined(MQTTMP)
		{
			int protocol = PROTOCOL_MQTT;
			if (client == NULL)
			{
				Listener* listener = Socket_getParentListener(sock);
				if (listener)
					protocol = listener->protocol;
			}
			else
				protocol = client->protocol;
			if (protocol == PROTOCOL_MQTT)
#endif
				MQTTProtocol_timeslice(sock, client);
#if defined(MQTTMP)
			else if (protocol == PROTOCOL_MQTT_MP)
				MQTTMPProtocol_timeslice(sock);
#endif
#if defined(MQTTS)
			else if (protocol == PROTOCOL_MQTTS)
			    MQTTSProtocol_timeslice(sock);
#endif
#if defined(MQTTS) || defined(MQTTMP)
		}
#endif
	}
	if (bstate->state != BROKER_STOPPING)
#if !defined(NO_ADMIN_COMMANDS)
		Persistence_read_command(bstate);
#else
		;
#endif
	else
		Protocol_closing();
	more_work = MQTTProtocol_housekeeping(more_work);
exit:
	FUNC_EXIT;
}


/**
 * Compares a clientid with the start of string b, up to the length of clientid.  For use in matching
 * $SYS/client/clientid/... topics
 * @param a pointer to a Clients structure
 * @param pointer to a string which may start with a clientid
 */
int clientIDPrefixCompare(void* a, void* b)
{
	Clients* client = (Clients*)a;
	int idlen = strlen(client->clientID);
	char* bc = (char*)b;

	/* printf("comparing clientdIDs %s with %s\n", client->clientID, (char*)b); */
	return strncmp(client->clientID, bc, idlen) == 0 && (strlen(bc) == idlen || bc[idlen] == '/');
}


/**
 * Originates a new publication - sends it to all clients subscribed.
 * @param publish pointer to a stucture which contains all the publication information
 * @param originator the originating client
 */
void Protocol_processPublication(Publish* publish, char* originator)
{
	Messages* stored = NULL; /* to avoid duplication of data where possible */
	List* clients = NULL;
	ListElement* current = NULL;
	int savedMsgId = publish->msgId;
	int clean_needed = 0;

	FUNC_ENTRY;
	if (Topics_hasWildcards(publish->topic))
	{
		Log(LOG_INFO, 12, NULL, publish->topic, originator);
		goto exit;
	}

	if ( (strcmp(INTERNAL_CLIENTID,originator)!=0) && bstate->password_file && bstate->acl_file)
	{
		Clients* client = (Clients*)(ListFindItem(bstate->clients, originator, clientIDCompare)->content);
		if (Users_authorise(client->user,publish->topic,ACL_WRITE) == false)
		{
			Log(LOG_AUDIT, 149, NULL, originator, publish->topic);
			goto exit;
		}
	}

	if (publish->header.bits.retain)
	{
		SubscriptionEngines_setRetained(bstate->se, publish->topic, publish->header.bits.qos, publish->payload, publish->payloadlen);
		if (bstate->persistence == 1 && bstate->autosave_on_changes == 1 && bstate->autosave_interval > 0
			&& bstate->se->retained_changes >= bstate->autosave_interval)
		{
			Log(LOG_INFO, 100, NULL, bstate->autosave_interval);
			SubscriptionEngines_save(bstate->se);
		}
	}

	if (strncmp(publish->topic, "$SYS/all-clients", 16) == 0)
	{
		ListElement* current = NULL;

		clients = ListInitialize();
		ListNextElement(bstate->clients, &current);
		while (current)
		{
			Clients* client = (Clients*)(current->content);
			Subscriptions* rcs = malloc(sizeof(Subscriptions));

			ListNextElement(bstate->clients, &current);
			rcs->clientName = client->clientID;
			rcs->qos = 2;
			rcs->priority = publish->priority;
			rcs->topicName = publish->topic;
			ListAppend(clients, rcs, sizeof(Subscriptions));
		}
	}
	else
	{
		clients = SubscriptionEngines_getSubscribers(bstate->se, publish->topic, originator);
		/* if the topic we are publishing to starts with $SYS/client/clientid/, then send this message direct to
		 * clients with matching clientids, in addition to any with matching subscriptions.
		 */
		if (strncmp(publish->topic, "$SYS/client/", 12) == 0)
		{
			ListElement* elem = ListFindItem(bstate->clients, &publish->topic[12], clientIDPrefixCompare);

			if (elem && elem->content)
			{
				Clients* client = (Clients*)(elem->content);

				clients->current = NULL;
				if (ListFindItem(clients, client->clientID, subsClientIDCompare) == NULL)
				{
					Subscriptions* rcs = malloc(sizeof(Subscriptions));
					rcs->clientName = client->clientID;
					rcs->qos = 2;
					rcs->priority = publish->priority;
					rcs->topicName = publish->topic;
					ListAppend(clients, rcs, sizeof(Subscriptions));
				}
			}
		}
	}

	current = NULL;
	while (ListNextElement(clients, &current))
	{
		Clients* pubclient = NULL;
		unsigned int qos = ((Subscriptions*)(current->content))->qos;
		int priority = ((Subscriptions*)(current->content))->priority;
		char* clientName = ((Subscriptions*)(current->content))->clientName;

		if (publish->header.bits.qos < qos) /* reduce qos if > subscribed qos */
			qos = publish->header.bits.qos;
		if (priority == PRIORITY_NORMAL) /* if outbound priority is not set, use inbound */
			priority = publish->priority;
		pubclient = (Clients*)(ListFindItem(bstate->clients, clientName, clientIDCompare)->content);
		if (pubclient != NULL)
		{
			int retained = 0;
			Messages* saved = NULL;
			char* original_topic = publish->topic;

#if !defined(NO_BRIDGE)
			if (pubclient->outbound || pubclient->noLocal)
			{
				retained = publish->header.bits.retain; /* outbound and noLocal mean outward/inward bridge client,
																							so keep retained flag */
				if (pubclient->outbound)
				{
					Bridge_handleOutbound(pubclient, publish);
					if (publish->topic != original_topic)
					{ /* handleOutbound has changed the topic, so we musn't used the stored pub which
					        contains the original topic */
						saved = stored;
						stored = NULL;
					}
				}
			}
#endif
			if (MQTTProtocol_startOrQueuePublish(pubclient, publish, qos, retained, priority, &stored) == SOCKET_ERROR)
			{
				pubclient->good = pubclient->connected = 0; /* flag this client as needing to be cleaned up */
				clean_needed = 1;
			}
			if (publish->topic != original_topic)
			{
				stored = saved;  /* restore the stored pointer for the next loop iteration */
				free(publish->topic);
				publish->topic = original_topic;
			}
		}
	}
	publish->msgId = savedMsgId;
	/* INTERNAL_CLIENTID means that we are publishing data to the log,
			and we don't want to interfere with other close processing */
	if (clean_needed && strcmp(originator, INTERNAL_CLIENTID) != 0)
		MQTTProtocol_clean_clients(bstate->clients);
	ListFree(clients);
exit:
	FUNC_EXIT;
}


#if defined(MQTTS)
Clients* Protocol_getclientbyaddr(char* addr)
{
	Clients* client = NULL;

	FUNC_ENTRY;
	if (ListFindItem(bstate->clients, addr, clientAddrCompare)!=NULL)
		client = (Clients*)(bstate->clients->current->content);
	FUNC_EXIT;
	return client;
}

#if !defined(NO_BRIDGE)
Clients* Protocol_getoutboundclient(int sock)
{
	Clients* client = NULL;

	FUNC_ENTRY;
	if (ListFindItem(bstate->clients, &sock, clientSocketCompare) != NULL)
		client = (Clients*)(bstate->clients->current->content);
	FUNC_EXIT;
	return client;
}
#endif
#endif


/**
 * Compare clients by client id prefix - used for simple authentication
 * @param prefix the client id prefix match
 * @param clientid the client id to match against
 * @return boolean - a flag to indicate whether the prefix matches the client id
 */
int clientPrefixCompare(void* prefix, void* clientid)
{
	/* printf("comparing prefix %s with clientid %s\n", (char*)prefix, (char*)clientid); */
	return strncmp((char*)prefix, (char*)clientid, strlen(prefix)) == 0;
}


/**
 * Check to see if a client should accept new work
 * @param client the client to check
 * @return boolean value to indicate whether the client should accept new work
 */
int Protocol_isClientQuiescing(Clients* client)
{
	return (bstate->state != BROKER_RUNNING ||
		(client->outbound && ((BridgeConnections*)client->bridge_context)->state != CONNECTION_RUNNING));
}
