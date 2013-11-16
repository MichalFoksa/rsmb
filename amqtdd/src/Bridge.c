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

/**
 * @file
 * Bridge functions
 */


#if !defined(NO_BRIDGE)

#include "Bridge.h"
#include "Log.h"
#include "MQTTProtocol.h"
#include "StackTrace.h"

#if defined(MQTTS)
#include "MQTTSProtocol.h"
#endif

#include "Topics.h"
#include "Messages.h"
#include "Protocol.h"

#include <string.h>
#include <stdio.h>

#include "Heap.h"


void Bridge_transmissionControl(BridgeConnections* bc);
void Bridge_runConnection(BridgeConnections* bc);
void Bridge_deleteConnection1(BridgeConnections* bc);

void Bridge_subscribe(BridgeConnections* bc, Clients* client);

static Bridges* bridge = NULL;					/**< bridge state structure */
static SubscriptionEngines* subsengine = NULL;	/**< subscription engine state structure */

#if !defined(min)
#define min(A,B) ( (A) < (B) ? (A):(B))
#endif


/**
 * Initialize a bridge
 * @param br pointer to a bridge state structure
 * @param se pointer to a subscriptions engine state structure
 */
void Bridge_initialize(Bridges* br, SubscriptionEngines* se)
{
	FUNC_ENTRY;
	bridge = br;
	subsengine = se;
	FUNC_EXIT;
}


/**
 * Stop the bridge.
 * @param bridge pointer to the bridge state structure
 */
void Bridge_stop(Bridges* bridge)
{
	ListElement* curelement = NULL;

	FUNC_ENTRY;
	if (bridge->connections != NULL)
	{
		while (ListNextElement(bridge->connections, &curelement))
			((BridgeConnections*)(curelement->content))->state = CONNECTION_STOPPING;
	}
	FUNC_EXIT;
}


/**
 * Create a new connection for a bridge, allocating storage appropriately
 * @param name the name of the connection
 * @return the new bridge connection state structure
 */
BridgeConnections* Bridge_new_connection(Bridges* bridge, char* name)
{
	BridgeConnections* bc = NULL;
	ListElement* curelement = NULL;

	FUNC_ENTRY;
	if (bridge->connections != NULL)
	{
		while (ListNextElement(bridge->connections, &curelement))
			if (strcmp(((BridgeConnections*)(curelement->content))->name, name) == 0)
			{
				Log(LOG_WARNING, 144, NULL, name);
				goto exit; /* already a connection with the same name */
			}
	}

	bc = malloc(sizeof(BridgeConnections));
	memset(bc, '\0', sizeof(BridgeConnections));
	bc->name = malloc(strlen(name)+1);
	strcpy(bc->name, name);
	bc->notifications = bc->try_private = 1;
	bc->topics = ListInitialize();
	bc->addresses = ListInitialize();
	bc->keepalive_interval = 60;
	bc->cleansession = -1;
	bc->idle_timeout = 60;
	bc->threshold = 10;
	bc->connect_timeout = 30;
	bc->reconnect_interval = bc->max_reconnect_interval = bc-> start_reconnect_interval = -1;
exit:
	FUNC_EXIT;
	return bc;
}


/**
 * Free all storage for a bridge connection structure
 * @param bc pointer to a bridge connection structure
 */
void Bridge_freeConnection(BridgeConnections* bc)
{
	ListElement* curtopic = NULL;

	FUNC_ENTRY;
	if (bc->primary)
	{
		bc->primary->cleansession = 1;
		MQTTProtocol_closeSession(bc->primary, 0);
	}

	if (bc->backup)
	{
		bc->backup->cleansession = 1;
		MQTTProtocol_closeSession(bc->backup, 0);
	}

	if (bc->name)
		free(bc->name);
	if (bc->username)
		free(bc->username);
	if (bc->password)
		free(bc->password);
	if (bc->clientid)
		free(bc->clientid);

	ListFree(bc->addresses);
	while (ListNextElement(bc->topics, &curtopic))
	{
		BridgeTopics* topic = (BridgeTopics*)(curtopic->content);
		free(topic->pattern);
		if (topic->localPrefix)
			free(topic->localPrefix);
		if (topic->remotePrefix)
			free(topic->remotePrefix);
	}
	ListFree(bc->topics);
	if (bc->notification_topic)
		free(bc->notification_topic);
	FUNC_EXIT;
}


/**
 * Terminate a bridge
 * @param br pointer to a bridge state structure
 */
void Bridge_terminate(Bridges* br)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	if (br->connections != NULL)
	{
		while (ListNextElement(br->connections, &current))
		{
			BridgeConnections* bc = (BridgeConnections*) (current->content);
			bc->state = CONNECTION_DELETE;
			Bridge_freeConnection(bc);
		}
		ListFree(br->connections);
	}
	FUNC_EXIT;
}


/**
 * Timeslice function for running the bridge.
 * Must take the minimum amount of time as it is for time slicing.
 * @param bridge pointer to the bridge state structure
 */
void Bridge_timeslice(Bridges* bridge)
{
	ListElement* curelement = NULL;

	FUNC_ENTRY;
	if (bridge->connections != NULL)
	{
		static time_t last_tc = 0;
		time_t now;

		time(&(now));
		if (difftime(now, last_tc) > 5)
		{
			last_tc = now;
			ListNextElement(bridge->connections, &curelement);
			while (curelement)
			{
				BridgeConnections* bc = (BridgeConnections*)(curelement->content);
				ListNextElement(bridge->connections, &curelement);
				Bridge_transmissionControl(bc); /* a connection can be deleted here */
			}
			curelement = NULL;
		}

		while (ListNextElement(bridge->connections, &curelement))
		{
			BridgeConnections* bc = (BridgeConnections*)(curelement->content);

			if (bc->state != CONNECTION_STOPPED)
				Bridge_runConnection(bc);
		}
	}
	FUNC_EXIT;
}


int Bridge_clientIDCompare(void* a, void* b)
{
	BridgeConnections* bc = (BridgeConnections*)a;
	if (bc->primary == NULL)
		return 0;
	else
		return strcmp(bc->primary->clientID, (char*)b) == 0;
}

/**
 * Startup a bridge connection for the first time.
 * @param bc the bridge connection state structure
 * @param pclient (pointer to a) pointer to a client structure, which is returned
 */
void Bridge_newConnection(BridgeConnections* bc, Clients** pclient)
{
	char* clientid = NULL;
	willMessages* wm = NULL;
	int cleansession = 0;
	char* dot = NULL;
	char* addr = NULL;

	FUNC_ENTRY;
	/* the backup client id is not actually used to connect, but must be different from the primary
	 * as clients are indexed by client id
	 */
	clientid = malloc(MAX_CLIENTID_LEN+1);

	if (bc->clientid)
	{
		strncpy(clientid, bc->clientid, MAX_CLIENTID_LEN);
		if (pclient == &(bc->backup))
			clientid[0] = '-';
	}
	else
	{
		#define MAX_HOSTNAME_LENGTH_USE 14
		strncpy(clientid, Socket_gethostname(), MAX_HOSTNAME_LENGTH_USE); /* don't fill the client id with the hostname */
		clientid[MAX_HOSTNAME_LENGTH_USE] = '\0'; /* make sure we terminate a partial hostname copy */
		if ((dot = strchr(clientid, '.')) != NULL) /* only use up to the first '.' */
			*dot = '\0';
		strncat(clientid, (pclient == &(bc->backup)) ? "-" : ".", MAX_CLIENTID_LEN - strlen(clientid));
		strncat(clientid, bc->name, MAX_CLIENTID_LEN - strlen(clientid));
	}
	clientid[MAX_CLIENTID_LEN] = '\0'; /* strncpy, strncat don't always null terminate */
	if (pclient == &(bc->primary))
		bc->cur_address = bc->addresses->first;
	Log(LOG_INFO, (pclient == &(bc->backup)) ? 123 : 124, NULL, bc->name);

	if (ListFindItem(bridge->connections, clientid, Bridge_clientIDCompare))
	{
		Log(LOG_WARNING, 56, NULL, bc->name, clientid);
		bc->state = CONNECTION_STOPPING_THEN_DELETE;
		free(clientid);
		goto exit;
	}
	bc->last_connect_result = CONNACK_NONE_RECEIVED;
	if (bc->notifications) 		/* if notifications are on, set the will message */
	{
		char* msg = "0"; /* 0 means disconnected */
		char* topic = "$SYS/broker/connection/%s/state";
		wm = (willMessages*)malloc(sizeof(willMessages));

		if (bc->notification_topic)
			topic = bc->notification_topic;
		wm->msg = malloc(strlen(msg)+1);
		strcpy(wm->msg, msg);
		if (strstr(topic, "%s"))
		{
			wm->topic = malloc(strlen(topic)+strlen(clientid)+1);
			sprintf(wm->topic, topic, clientid);
		}
		else
		{
			wm->topic = malloc(strlen(topic)+1);
			strcpy(wm->topic, topic);
		}
		wm->retained = 0;
		wm->qos = 0;
	}
	if (bc->cleansession == -1)
		/* default is to set cleansession if there is more than one address */
		cleansession = (bc->addresses->count > 1);
	else
		cleansession = bc->cleansession;
	if (bc->round_robin == 0 && pclient == &(bc->backup))
		addr = (char*)(bc->addresses->first->content);
	else
		addr = (char*)(bc->cur_address->content);
#if defined(MQTTS)
	if (bc->protocol == PROTOCOL_MQTTS)
		*pclient = MQTTSProtocol_connect(addr, clientid,
				cleansession, bc->try_private, max(bc->keepalive_interval, 5), wm);
	else
#endif
	*pclient = MQTTProtocol_connect(addr, clientid,
			cleansession, bc->try_private, max(bc->keepalive_interval, 5), wm,
			bc->username, bc->password);

	if (*pclient == NULL)
	{
		free(clientid);
		Log(LOG_WARNING, 125, NULL);
	}
	else
		(*pclient)->bridge_context = bc;
		
	if (bc->start_reconnect_interval < 0)
		bc->reconnect_interval = bc->start_reconnect_interval = 20;
	else if (bc->max_reconnect_interval > bc->start_reconnect_interval)
	{	  
		/* random number between min and max */
		bc->reconnect_interval = rand() % (bc->max_reconnect_interval - bc->start_reconnect_interval + 1) + bc->start_reconnect_interval;
		bc->chosen_reconnect_interval = bc->reconnect_interval - bc->start_reconnect_interval;
	}
	else
		bc->reconnect_interval = bc->start_reconnect_interval;
	
exit:
	FUNC_EXIT;
}


int pow2(int exp)
{
	if (exp == 0)
		return 1;
	return pow2(exp - 1) * 2;
}

int end_interval(int count, int start, int max)
{
	if (count == 0)
		return start;
	else if (count == 1)
		return max;
	else
		return end_interval(count - 1, start, max) + (max - start)*pow2(count - 1);
}


int next_interval(int count, int start, int max, int chosen)
{
	if (count > 3)
		count = 3;
	return end_interval(count - 1, start, max) + chosen*pow2(count - 1);
}


void Bridge_nextRetry(BridgeConnections* bc, Clients* client)
{
	FUNC_ENTRY;
	if (bc->try_private && (bc->last_connect_result == CONNACK_NONE_RECEIVED || 
	    bc->last_connect_result == CONNACK_UNACCEPTABLE_PROTOCOL_VERSION) && client->noLocal)
	    goto exit;
	    
	/*set next retry interval in case of failure */
	if (bc->max_reconnect_interval > bc->start_reconnect_interval)
		bc->reconnect_interval = next_interval((++bc->reconnect_count), bc->start_reconnect_interval,
	                                    bc->max_reconnect_interval, bc->chosen_reconnect_interval);
	else
	{	
		bc->reconnect_count++;
		bc->reconnect_interval = bc->start_reconnect_interval * pow2(min(bc->reconnect_count, 3) - 1);
	}
	Log(LOG_INFO, 154, "Bridge connection %s will be retried in %d seconds", bc->name, bc->reconnect_interval);
	time(&(client->lastContact));
exit:
	FUNC_EXIT;
}


/**
 * Startup a bridge connection for the second or subsequent time.
 * @param bc the bridge connection state structure
 * @param pclient (pointer to a) pointer to a client structure, which is returned
 */
void Bridge_restartConnection(BridgeConnections* bc, Clients** pclient)
{
	time_t now;

	FUNC_ENTRY;
	time(&(now));
	if (bc->try_private && (bc->last_connect_result == CONNACK_NONE_RECEIVED || 
	    bc->last_connect_result == CONNACK_UNACCEPTABLE_PROTOCOL_VERSION) && (*pclient)->noLocal)
	{
		char* addr;
		(*pclient)->noLocal = 0;
		if (bc->round_robin == 0 && pclient == &(bc->backup))
			addr = (char*)(bc->addresses->first->content);
		else
			addr = (char*)(bc->cur_address->content);
		Log(LOG_INFO, 99, NULL, bc->name, addr);
#if defined(MQTTS)
		if (bc->protocol == PROTOCOL_MQTTS)
			MQTTSProtocol_reconnect(addr,(*pclient));
		else
#endif
		MQTTProtocol_reconnect(addr, (*pclient));
	}
	else if (difftime(now, (*pclient)->lastContact) > bc->reconnect_interval)
	{
		ListElement* addr = NULL;
		
		if (bc->try_private && (*pclient)->noLocal == 0)
			(*pclient)->noLocal = 1;
		if (pclient == &(bc->backup))
			addr = bc->addresses->first;
		else
		{
			if (ListNextElement(bc->addresses, &(bc->cur_address)) == NULL)
				bc->cur_address = bc->addresses->first;
			addr = bc->cur_address;
		}
		bc->last_connect_result = CONNACK_NONE_RECEIVED;
		Log(LOG_INFO, 127, NULL, bc->name, (char*)(addr->content));
#if defined(MQTTS)
		if (bc->protocol == PROTOCOL_MQTTS)
			MQTTSProtocol_reconnect((char*)(addr->content),(*pclient));
		else
#endif
			MQTTProtocol_reconnect((char*)(addr->content), (*pclient));
	}
	FUNC_EXIT;
}


/**
 * Time slice the running of a bridge connection primary or backup client.
 * @param bc the bridge connection state structure
 * @param pclient (pointer to a) pointer to a client structure, which is returned
 */
void Bridge_processPartConnection(BridgeConnections* bc, Clients** pclient)
{
	FUNC_ENTRY;
	if (*pclient == NULL)
	{
		if (bc->state == CONNECTION_RUNNING && bc->addresses->first != NULL)
			Bridge_newConnection(bc, pclient);		/* create an outbound connection */
	}
	else
	{
		Log(TRACE_MAX, 28, NULL, bc->name, (*pclient)->connected, (*pclient)->connect_state);
		if ((*pclient)->connected == 0 && (*pclient)->connect_state == 0)
		{
			if (bc->state == CONNECTION_RUNNING)
			{
				if (bc->start_type == START_ONCE)
					bc->state = CONNECTION_STOPPING_THEN_DELETE;
				else
					Bridge_restartConnection(bc, pclient);
			}
		}
		else if ((*pclient)->connected == 0 && (*pclient)->connect_state > 0)
		{
			time_t now;

			time(&(now));
			/* the time allowed before a connect must complete now defaults to the keepalive interval */
			if (difftime(now, (*pclient)->lastContact) > bc->connect_timeout)
			{
				Log(LOG_INFO, 128, NULL, bc->name);
				MQTTProtocol_closeSession((*pclient), 0);
				Bridge_nextRetry(bc, *pclient);
			}
		}
#if defined(MQTTS)
		else if ((*pclient)->connected && (*pclient)->connect_state == 3 &&
				!bc->completed_subscriptions)
			Bridge_subscribe(bc, *pclient);
#endif
	}
	FUNC_EXIT;
}


/**
 * Time slice the running of a bridge connection.
 * @param bc the bridge connection state structure
 */
void Bridge_runConnection(BridgeConnections* bc)
{
	FUNC_ENTRY;
	Bridge_processPartConnection(bc, &(bc->primary));

	if (bc->round_robin == 0 && bc->addresses->count > 1 && /* basic conditions */
		(bc->primary != NULL && bc->primary->connected == 1 ) && /* primary successfully connected */
		bc->cur_address != bc->addresses->first)	/* primary is not using the main address, which is first in the list */
		Bridge_processPartConnection(bc, &(bc->backup));
	FUNC_EXIT;
}


/**
 * Check if a client is stopped so that it is ready to be cleaned up.
 * @param client pointer to the client to be checked
 * @return boolean value to indicate if the client is stopped
 */
int Bridge_isClientStopped(Clients* client)
{
	int stopped = 1;

	FUNC_ENTRY;
	if (client)
	{
		stopped = 0;
		if (client->connect_state > 0 || (client->connected && !Protocol_inProcess(client)))
			MQTTProtocol_closeSession(client, 0);
		if (client->connected == 0 && client->connect_state == 0)
			stopped = 1;
	}
	FUNC_EXIT_RC(stopped);
	return stopped;
}


/**
 * Transmission control for a bridge connection - start or stop it.
 * @param bc pointer to the bridge connection state structure
 */
void Bridge_transmissionControl(BridgeConnections* bc)
{
	FUNC_ENTRY;
	if (bc->state == CONNECTION_STOPPED)
	{ /* see if we need to start the connection */
		if (bc->start_type == START_LAZY)
		{
			if (queuedMsgsCount(bc->primary) >= bc->threshold)
				bc->state = CONNECTION_RUNNING;
		}
		else if (bc->start_type == START_AUTOMATIC)
		{
			if (!bc->stop_was_manual)
				bc->state = CONNECTION_RUNNING;
		}
		else if (bc->start_type == START_ONCE && bc->no_successful_connections == 0)
			bc->state = CONNECTION_RUNNING;
	}
	else if (bc->state == CONNECTION_RUNNING)
	{ /* see if we need to stop the connection */
		if (bc->start_type == START_LAZY)
		{
			if (bc->state == CONNECTION_RUNNING && bc->primary->lastContact > bc->idle_timeout)
			{
				Log(LOG_INFO, 63, NULL, bc->name);
				bc->state = CONNECTION_STOPPING;
			}
		}
		if (bc->primary->connected && bc->cur_address != bc->addresses->first && bc->backup && bc->backup->connect_state == 2)
			/* time to switch from backup to primary */
			bc->state = CONNECTION_SWITCHING;
	}
	else if (bc->state == CONNECTION_STOPPING || bc->state == CONNECTION_STOPPING_THEN_DELETE)
	{
		/* check through all the clients for this connection to see if they are stopped */
		if (Bridge_isClientStopped(bc->primary) && Bridge_isClientStopped(bc->backup))
		{
			Log(LOG_INFO, 62, NULL, bc->name);
			if (bc->state == CONNECTION_STOPPING)
				bc->state = CONNECTION_STOPPED;
			else
				Bridge_deleteConnection1(bc);
		}
	}
	else if (bc->state == CONNECTION_SWITCHING)
	{
		if (Bridge_isClientStopped(bc->primary))
		{
			Log(LOG_INFO, 0, "Connection %s switching back to main address", bc->name);
			bc->primary->socket = bc->backup->socket;
			bc->primary->connect_state = bc->backup->connect_state;
			bc->cur_address = bc->addresses->first;
			bc->state = CONNECTION_RUNNING;
			bc->backup->connect_state = bc->backup->socket = 0;
			MQTTPacket_send_connect(bc->primary);
		}
	}
	FUNC_EXIT;
}


/**
 * Handle intermediate connect processing for a bridge connection.
 * @param client the bridge connection state structure
 */
void Bridge_handleConnection(Clients* client)
{
	int error;
	socklen_t len = sizeof(error);
	ListElement* elem = NULL;
	BridgeConnections* bc = NULL;

	FUNC_ENTRY;
	while (ListNextElement(bridge->connections, &elem))
	{
		bc = (BridgeConnections*)elem->content;
		if (client == bc->primary || client == bc->backup)
		  break;
		bc = NULL;
	}

	/* check the socket state, if it's ok then we can send the connect packet */
	if (getsockopt(client->socket, SOL_SOCKET, SO_ERROR, (char*)&error, &len) != 0)
	{
		Log(LOG_WARNING, 129, NULL);
		MQTTProtocol_closeSession(client, 0);
		Bridge_nextRetry(bc, client);
	}
	else if (error != 0)
	{
		char* addr = (client == bc->primary) ? bc->cur_address->content : bc->addresses->first->content;
		Log(LOG_WARNING, 130, NULL, client->clientID, addr, error);
		MQTTProtocol_closeSession(client, 0);
		Bridge_nextRetry(bc, client);
	}
	else
	{
		int rc;
		if (client == bc->backup)
			client->connect_state = 2;
		else
		{
			rc = MQTTPacket_send_connect(client);
			if (rc == TCPSOCKET_COMPLETE)
				client->connect_state = 2;
			else
			{
				char* addr = (client == bc->primary) ? bc->cur_address->content : bc->addresses->first->content;
				Log(LOG_WARNING, 130, NULL, client->clientID, addr, rc);
				MQTTProtocol_closeSession(client, 0);
				Bridge_nextRetry(bc, client);
			}
		}
	}
	FUNC_EXIT;
}


/**
 * Add prefix to a topic string, allocating storage appropriately.
 * @param pattern the topic to add the prefix to
 * @param prefix the prefix to add
 * @param newlen the returned length of the allocated string
 * @return the newly created string
 */
char* Bridge_addPrefix(char* pattern, char* prefix, int* newlen)
{
	char* newtopic;

	FUNC_ENTRY;
	*newlen = strlen(pattern) + 1;
	if (prefix)
		*newlen += strlen(prefix);
	newtopic = malloc(*newlen);
	if (prefix)
	{
		strcpy(newtopic, prefix);
		strcat(newtopic, pattern);
	}
	else
		strcpy(newtopic, pattern);
	FUNC_EXIT;
	return newtopic;
}


/**
 *  Called after a bridge connection has been established to subscribe to appropriate topics.
 *  @param bc pointer to a bridge connection
 *  @param client pointer to a client structure
 */
void Bridge_subscribe(BridgeConnections* bc, Clients* client)
{
#if defined(MQTTS)
	FUNC_ENTRY;
	if (client->protocol == PROTOCOL_MQTT)
	{
#endif
		List* topics = ListInitialize();
		List* qoss = ListInitialize();
		ListElement* elem = NULL;
#if !defined(MQTTS)
		FUNC_ENTRY;
#endif
#if defined(MQTTS)
		bc->completed_subscriptions = 1;
#endif
		while (ListNextElement(bc->topics, &elem))
		{
			int len;
			BridgeTopics* curtopic = (BridgeTopics*)(elem->content);

			if (curtopic->direction == 0 || curtopic->direction == 1)              /* both=0, in=1, out=2 */
			{	/* remote subscription */
				int* qos;
				char* fulltopic = Bridge_addPrefix(curtopic->pattern, curtopic->remotePrefix, &len);
				ListAppend(topics, fulltopic, len);
				qos = malloc(sizeof(int));
				*qos = 2;
				ListAppend(qoss, qos, sizeof(int));
			}
			if (curtopic->direction == 0 || curtopic->direction == 2)
			{
				char* fulltopic = Bridge_addPrefix(curtopic->pattern, curtopic->localPrefix, &len);
				if (!Topics_isValidName(fulltopic))
				{
					Log(LOG_WARNING, 153, NULL, fulltopic, client->clientID, client->addr);
					free(fulltopic);
				}
				else
				{
					int isnew = SubscriptionEngines_subscribe(subsengine, client->clientID,				 /* local subscription */
						fulltopic, 2, 1, (client->cleansession == 0), curtopic->priority); /* this is noLocal (and keep retained flags) */
					if (isnew || client->cleansession == 1)
					/* retained messages only sent if the subscription was new */
						MQTTProtocol_processRetaineds(client, fulltopic, 2, curtopic->priority);
				}
			}
		}
		if (topics->count > 0)
			MQTTProtocol_subscribe(client, topics, qoss);
		ListFree(topics);
		ListFree(qoss);
#if defined(MQTTS)
	}
	else
	{
		ListElement* elem = NULL;
		bc->completed_subscriptions = 1;
		while (ListNextElement(bc->topics, &elem))
		{
			int len;
			BridgeTopics* curtopic = (BridgeTopics*)(elem->content);
			if (!curtopic->subscribed)
			{
				if (curtopic->direction == 0 || curtopic->direction == 1) /* both=0, in=1, out=2 */
				{
					if (client->pendingSubscription == NULL)
					{
						char* fulltopic = Bridge_addPrefix(curtopic->pattern, curtopic->remotePrefix, &len);
						bc->completed_subscriptions = 0;
						/* TODO handle rc of startSubscription */
						MQTTSProtocol_startSubscription(client, fulltopic, 2);
					}

				}
				if (curtopic->direction == 0 || curtopic->direction == 2)
				{
					char* fulltopic = Bridge_addPrefix(curtopic->pattern, curtopic->localPrefix, &len);
					if (SubscriptionEngines_subscribe(subsengine, client->clientID,				 /* local subscription */
							fulltopic, 2, 1, (client->cleansession == 0), curtopic->priority)) /* this is noLocal (and keep retained flags) */
						/* retained messages only sent if the subscription was new */
						MQTTProtocol_processRetaineds(client, fulltopic, 2, curtopic->priority);
					curtopic->subscribed = 1;
				}
			}
		}
	}
#endif
	FUNC_EXIT;
}


/**
 * Handling the connack packet for a bridge connection.
 * @param pack the connack packet
 * @param sock the socket on which the packet was received
 * @return completion code - TCPSOCKET_COMPLETE is success
 */
int Bridge_handleConnacks(void* pack, int sock)
{
	Connack* connack = (Connack*)pack;
	ListElement* elem = NULL;
	BridgeConnections* bc = NULL;
	Clients* client = NULL;
	int rc = SOCKET_ERROR;

	FUNC_ENTRY;
	if (bridge->connections == NULL)
	{
		free(connack);
		FUNC_EXIT_RC(rc);
		return rc;
	}

	while (ListNextElement(bridge->connections, &elem))
	{
		bc = (BridgeConnections*)elem->content;
		if (bc->primary && bc->primary->socket == sock)
		{
			client = bc->primary;
			break;
		}
		if (bc->backup && bc->backup->socket == sock)
		{
			client = bc->backup;
			break;
		}
		bc = NULL;
	}

	if (client == NULL)
	{
		free(connack);
		FUNC_EXIT_RC(rc);
		return rc;
	}

	Log(LOG_PROTOCOL, 1, NULL, client->socket, client->clientID, connack->rc);

	if (connack->rc != CONNACK_CONNECTION_ACCEPTED)
	{
		if (client)
		{
			if (connack->rc != CONNACK_UNACCEPTABLE_PROTOCOL_VERSION || client->noLocal != 1)
				Log(LOG_WARNING, 132, NULL, connack->rc, client->clientID);
			MQTTProtocol_closeSession(client, 0);

			bc->last_connect_result = connack->rc;
		}
		else
			Socket_close(sock);
	}
	else if (client) /* now we can issue the subscriptions, both local and remote */
	{
		Log(LOG_INFO, 133, NULL, bc->name, bc->cur_address->content);
		if (client->will)
		{
			Publish pub;
			MQTTProtocol_sys_publish(client->will->topic, "1");
			pub.payload = "1";
			pub.payloadlen = 1;
			pub.topic = client->will->topic;
			MQTTPacket_send_publish(&pub, 0, 0, 1, client->socket, client->clientID);
			time(&(client->lastContact));
		}

		client->connect_state = 3;  /* should have been 2 before */
		bc->last_connect_result = CONNACK_CONNECTION_ACCEPTED;
		(bc->no_successful_connections)++;
		client->connected = 1;
		client->good = 1;
		client->ping_outstanding = 0;
		bc->reconnect_count = 0;
		bc->reconnect_interval = bc->chosen_reconnect_interval + bc->start_reconnect_interval;

		/* if (bc->addresses->count > 1 || bc->no_successful_connections == 1) */
			Bridge_subscribe(bc, client);
	}

	free(connack);
	rc = TCPSOCKET_COMPLETE;
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process topic strings for messages on "mounted" bridge connections.
 * @param bc pointer to the bridge connection
 * @param publish the message to be processed
 * @param outbound out or inbound message?
 * @return the processed bridge topic
 */
BridgeTopics* Bridge_stripAndReplace(BridgeConnections* bc, Publish* publish, int outbound)
{
	ListElement* elem = NULL;
	BridgeTopics* match = NULL;

	FUNC_ENTRY;
	while (ListNextElement(bc->topics, &elem))
	{
		char* source;
		int len;
		char* prefix = NULL;

		BridgeTopics* bt = (BridgeTopics*)elem->content;
		if (outbound == 1 && bt->direction == 1)
			continue;
		if (outbound == 0 && bt->direction == 2)
			continue;

		prefix = outbound ? bt->localPrefix : bt->remotePrefix;
		if (prefix)
			source = Bridge_addPrefix(bt->pattern, prefix, &len);
		else
			source = bt->pattern;

		if (Topics_matches(source, publish->topic))
		{
			if (match)
			{
				if (SubscriptionEngines_mostSpecific(match->pattern, source) == source)
					match = bt;
			}
			else
				match = bt;
		}
		if (prefix)
			free(source);
	}

	if (match)
	{
		int matchSourcePrefixLen, matchDestPrefixLen;
		char* newtopic;
		char* matchSource = outbound ? match->localPrefix : match->remotePrefix;
		char* matchDest = outbound ? match->remotePrefix : match->localPrefix;

		matchSourcePrefixLen = (matchSource) ? strlen(matchSource) : 0;
		matchDestPrefixLen = (matchDest) ? strlen(matchDest) : 0;

		newtopic = malloc(strlen(publish->topic) - matchSourcePrefixLen + matchDestPrefixLen + 1);
		if (matchDest)
			strcpy(newtopic, matchDest); /* add destination prefix */
		else
			newtopic[0] = '\0';

		if (matchSource)
			strcat(newtopic, &(publish->topic)[matchSourcePrefixLen]); /* strip source prefix */
		else
			strcat(newtopic, publish->topic);

		if (!outbound)
			free(publish->topic);
		publish->topic = newtopic;
	}

	FUNC_EXIT;
	return match;
}


/**
 * Handle an inbound publish message for a bridge connection.
 * @param client the client on which the publish was received
 * @param publish the publish message itself
 */
void Bridge_handleInbound(Clients* client, Publish* publish)
{ /* transform the publish packet as per bridge function */
	BridgeConnections* bc = (BridgeConnections*)client->bridge_context;
	BridgeTopics* match = NULL;

	FUNC_ENTRY;
	if (bc->inbound_filter)
	{
		/* send this to the right destination an application to pre-process */

	}
	match = Bridge_stripAndReplace(bc, publish, 0);
	if (match)
		publish->priority = match->priority;
	else
		Log(LOG_WARNING, 135, NULL, publish->topic, bc->name);
	FUNC_EXIT;
}


/**
 * Handle an outbound publish message for a bridge connection.
 * @param client the client on which the publish was received
 * @param publish the publish message itself
 */
void Bridge_handleOutbound(Clients* client, Publish* publish)
{
	BridgeConnections* bc = (BridgeConnections*)client->bridge_context;

	FUNC_ENTRY;
#if 0
	if (bc->outbound_filter)
	{
		/* send this to a system topic for an application to pre-process */
	}
#endif

	if (Bridge_stripAndReplace(bc, publish, 1) == NULL)
		Log(LOG_WARNING, 136, NULL, publish->topic, bc->name);
	FUNC_EXIT;
}

#if defined(MQTTS)
BridgeConnections* Bridge_getBridgeConnection(int sock)
{
	ListElement* elem = NULL;
	BridgeConnections* bc = NULL;

	FUNC_ENTRY;
	if (bridge->connections)
	{
		while (ListNextElement(bridge->connections, &elem))
		{
			bc = (BridgeConnections*)elem->content;
			if (bc->primary && bc->primary->socket == sock)
				break;
			if (bc->backup && bc->backup->socket == sock)
				break;
			bc = NULL;
		}
	}
	FUNC_EXIT;
	return bc;
}

Clients* Bridge_getClient(int sock)
{
	Clients* client = NULL;
	BridgeConnections* bc = Bridge_getBridgeConnection(sock);

	FUNC_ENTRY;
	if (bc != NULL)
	{
		if (bc->primary && bc->primary->socket == sock)
			client = bc->primary;
		else
			client = bc->backup;
	}
	FUNC_EXIT;
	return client;
}
#endif



/**
 * Truly delete a connection when it has been stopped.
 * @param bc pointer to the bridge connection
 */
void Bridge_deleteConnection1(BridgeConnections* bc)
{
	char* tempname = bc->name;

	FUNC_ENTRY;
	bc->name = NULL;
	bc->state = CONNECTION_DELETE;
	Bridge_freeConnection(bc);
	if (ListRemove(bridge->connections, bc))
		Log(LOG_INFO, 57, NULL, tempname);
	else
		Log(LOG_WARNING, 58, NULL, tempname);
	free(tempname);
	FUNC_EXIT;
}


#if !defined(NO_ADMIN_COMMANDS)
/**
 * Find a bridge connection by name
 * @param name the name of the connection to find
 * @return pointer to a bridge connection state structure
 */
BridgeConnections* Bridge_findConnection(char* name)
{
	BridgeConnections* bc = NULL;

	FUNC_ENTRY;
	if (bridge->connections != NULL)
	{
		ListElement* curelement = NULL;
		while (ListNextElement(bridge->connections, &curelement))
		{
			if (strcmp(((BridgeConnections*)curelement->content)->name, name) == 0)
			{
				bc = (BridgeConnections*)curelement->content;
				break;
			}
		}
	}

	if (!bc)
		Log(LOG_INFO, 59, NULL, name);

	FUNC_EXIT;
	return bc;
}


/**
 * Start a bridge connection
 * @param name the name of the connection to start
 */
int Bridge_startConnection(char* name)
{
	BridgeConnections* bc = NULL;
	int rc = -1;

	FUNC_ENTRY;
	if ((bc = Bridge_findConnection(name)) != NULL && bc->state == CONNECTION_STOPPED)
	{
		bc->state = CONNECTION_RUNNING;
		bc->stop_was_manual = 0;
		rc = 0;
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Stop a bridge connection
 * @param name the name of the connection to stop
 */
int Bridge_stopConnection(char* name)
{
	BridgeConnections* bc = NULL;
	int rc = -1;

	FUNC_ENTRY;
	if ((bc = Bridge_findConnection(name)) != NULL)
	{
		if (bc->state == CONNECTION_RUNNING)
		{
			if (bc->start_type == START_ONCE)
				bc->state = CONNECTION_STOPPING_THEN_DELETE;
			else
				bc->state = CONNECTION_STOPPING;
			Log(LOG_INFO, 60, NULL, name);
			bc->stop_was_manual = 1;
			rc = 0;
		}
		else
			Log(LOG_WARNING, 61, NULL, name);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Delete a bridge connection
 * @param name the name of the connection to delete
 */
int Bridge_deleteConnection(char* name)
{
	BridgeConnections* bc = NULL;
	int rc = -1;

	FUNC_ENTRY;
	if ((bc = Bridge_findConnection(name)) != NULL)
	{
		if (bc->state == CONNECTION_RUNNING || bc->state == CONNECTION_STOPPING)
			bc->state = CONNECTION_STOPPING_THEN_DELETE;
		else if (bc->state == CONNECTION_STOPPED)
			Bridge_deleteConnection1(bc);
		rc = 0;
	}
	FUNC_EXIT_RC(rc);
	return rc;
}

#endif

#endif
