/*******************************************************************************
 * Copyright (c) 2008, 2013 IBM Corp.
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
 *    Nicholas O'Leary, Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

#if defined(MQTTS)

#include "MQTTSProtocol.h"
#include "MQTTSPacket.h"
#include "MQTTProtocol.h"
#include "Socket.h"
#include "Topics.h"
#include "Log.h"
#include "Messages.h"
#include "Protocol.h"
#include "StackTrace.h"

#if !defined(NO_BRIDGE)
#include "MQTTSProtocolOut.h"
#endif


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

#include "Heap.h"

void MQTTProtocol_removePublication(Publications* p);


typedef int (*pf)(void*, int, char*);

#if !defined(NO_BRIDGE)
static pf handle_packets[] =
{
		MQTTSProtocol_handleAdvertises,
		MQTTSProtocol_handleSearchGws,
		MQTTSProtocol_handleGwInfos,
		NULL,
		MQTTSProtocol_handleConnects,
		MQTTSProtocol_handleConnacks,
		MQTTSProtocol_handleWillTopicReqs,
		MQTTSProtocol_handleWillTopics,
		MQTTSProtocol_handleWillMsgReqs,
		MQTTSProtocol_handleWillMsgs,
		MQTTSProtocol_handleRegisters,
		MQTTSProtocol_handleRegacks,
		MQTTSProtocol_handlePublishes,
		MQTTSProtocol_handlePubacks,
		MQTTSProtocol_handlePubcomps,
		MQTTSProtocol_handlePubrecs,
		MQTTSProtocol_handlePubrels,
		NULL,
		MQTTSProtocol_handleSubscribes,
		MQTTSProtocol_handleSubacks,
		MQTTSProtocol_handleUnsubscribes,
		MQTTSProtocol_handleUnsubacks,
		MQTTSProtocol_handlePingreqs,
		MQTTSProtocol_handlePingresps,
		MQTTSProtocol_handleDisconnects,
		NULL,
		MQTTSProtocol_handleWillTopicUpds,
		MQTTSProtocol_handleWillTopicResps,
		MQTTSProtocol_handleWillMsgUpds,
		MQTTSProtocol_handleWillMsgResps
};
#else
static pf handle_packets[] =
{
		MQTTSProtocol_handleAdvertises,
		MQTTSProtocol_handleSearchGws,
		MQTTSProtocol_handleGwInfos,
		NULL,
		MQTTSProtocol_handleConnects,
		NULL, /* connack  */
		NULL, /* willtopicreq */
		MQTTSProtocol_handleWillTopics,
		NULL, /* willmsgreq */
		MQTTSProtocol_handleWillMsgs,
		MQTTSProtocol_handleRegisters,
		MQTTSProtocol_handleRegacks,
		MQTTSProtocol_handlePublishes,
		MQTTSProtocol_handlePubacks,
		MQTTSProtocol_handlePubcomps,
		MQTTSProtocol_handlePubrecs,
		MQTTSProtocol_handlePubrels,
		NULL,
		MQTTSProtocol_handleSubscribes,
		NULL, /* subacks */
		MQTTSProtocol_handleUnsubscribes,
		MQTTSProtocol_handleUnsubacks,
		MQTTSProtocol_handlePingreqs,
		MQTTSProtocol_handlePingresps,
		MQTTSProtocol_handleDisconnects,
		NULL,
		MQTTSProtocol_handleWillTopicUpds,
		MQTTSProtocol_handleWillTopicResps,
		MQTTSProtocol_handleWillMsgUpds,
		MQTTSProtocol_handleWillMsgResps
};
#endif

BrokerStates* bstate;


int registeredTopicNameCompare(void*a, void* b)
{
	Registration* reg = (Registration*)a;

	return strcmp(reg->topicName, (char*)b) == 0;
}
int registeredTopicIdCompare(void*a, void* b)
{
	Registration* reg = (Registration*)a;

	return reg->id == *(int*)b;
}


int MQTTSProtocol_initialize(BrokerStates* aBrokerState)
{
	bstate = aBrokerState;

	return 0;
}

void MQTTSProtocol_timeslice(int sock)
{
	int error;
	MQTTS_Header* pack;
	char* clientAddr;
	Clients* client = NULL;

	FUNC_ENTRY;
	pack = MQTTSPacket_Factory(sock, &clientAddr, &error);

	client = Protocol_getclientbyaddr(clientAddr);

#if !defined(NO_BRIDGE)
	if (client == NULL)
		client = Protocol_getoutboundclient(sock);
#endif

	if (pack == NULL)
	{
		if (error == SOCKET_ERROR || error == UDPSOCKET_INCOMPLETE)
		{
			if (client != NULL)
			{
				client->good = 0; /* make sure we don't try and send messages to ourselves */
				client->connected = 0;
				if (error == SOCKET_ERROR)
					Log(LOG_WARNING, 18, NULL, client->clientID, client->socket,
							Socket_getpeer(client->socket));
				else
					Log(LOG_WARNING, 19, NULL, client->clientID, client->socket,
							Socket_getpeer(client->socket));
				MQTTProtocol_closeSession(client, 0);
			}
			else
			{
				if (error == SOCKET_ERROR)
					Log(LOG_WARNING, 20, NULL, sock, Socket_getpeer(sock));
				else
					Log(LOG_WARNING, 21, NULL, sock, Socket_getpeer(sock));
				/*Socket_close(sock);*/
			}
		}
	}
	else if (handle_packets[(int)pack->header.type] == NULL)
	{
			Log(LOG_WARNING, 22, NULL, pack->header.type, handle_packets[(int)pack->header.type]);
			MQTTSPacket_free_packet(pack);
	}
	else if (client == NULL
			&&
			((pack->header.type == MQTTS_PUBLISH && ((MQTTS_Publish*)pack)->flags.QoS != 3)
			||
			(pack->header.type != MQTTS_PUBLISH && pack->header.type != MQTTS_CONNECT)))
	{
			Log(LOG_WARNING, 23, NULL, sock, MQTTSPacket_name(pack->header.type));
			MQTTSPacket_free_packet(pack);
	}
	else
	{
		(*handle_packets[(int)pack->header.type])(pack, sock, clientAddr);
		/* TODO:
		 *  - error handling
		 *  - centralise calls to time( &(c->lastContact) ); (currently in each _handle* function
		 */
	}

	FUNC_EXIT;
}

int MQTTSProtocol_handleAdvertises(void* pack, int sock, char* clientAddr){ return 0; }

int MQTTSProtocol_handleSearchGws(void* pack, int sock, char* clientAddr){ return 0; }

int MQTTSProtocol_handleGwInfos(void* pack, int sock, char* clientAddr){ return 0; }

int MQTTSProtocol_handleConnects(void* pack, int sock, char* clientAddr)
{
	MQTTS_Connect* connect = (MQTTS_Connect*)pack;
	Listener* list = NULL;
	int terminate = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	ListElement* elem = NULL;
	int rc = 0;
	int existingClient = 0;

	FUNC_ENTRY;
	if (bstate->clientid_prefixes->count > 0 &&
		!ListFindItem(bstate->clientid_prefixes, connect->clientID, clientPrefixCompare))
	{
		Log(LOG_WARNING, 31, NULL, connect->clientID);
		terminate = 1;
	}
	else
	{
		list = Socket_getParentListener(sock);
		if (list->max_connections > -1 &&
				list->connections->count > list->max_connections)
		{
			/* TODO: why is this commented out? delete if not needed
			//MQTTPacket_send_connack(3, sock);
			*/
			Log(LOG_WARNING, 141, NULL, connect->clientID, list->max_connections, list->port);
			terminate = 1;

		}
		else if (connect->protocolID != 1)
		{
			Log(LOG_WARNING, 32, NULL, "MQTT-S", connect->protocolID);
			/* TODO: why is this commented out? delete if not needed
			//MQTTPacket_send_connack(1, sock);
			 */
			terminate = 1;
		}
	}

	if (terminate)
	{
		/*TODO: process the terminate*/
		MQTTSPacket_free_packet(pack);
		goto exit;
	}

	if (client != NULL && !strcmp(client->clientID, connect->clientID))
	{
		/* Connect for a new client id on a used addr
		 * TODO: clean out 'old' Client (that may be 'connected')
		 */
	}

	elem = ListFindItem(bstate->clients, connect->clientID, clientIDCompare);
	if (elem == NULL)
	{
		Clients* newc = malloc(sizeof(Clients));
		newc->clientID = connect->clientID;
		newc->protocol = PROTOCOL_MQTTS;
		newc->cleansession = connect->flags.cleanSession;
		newc->outbound = 0;
		newc->socket = sock;
		newc->connected = 0; /* Do not connect until we know the connack has been sent */
		newc->keepAliveInterval = connect->keepAlive;
		newc->msgID = 0;
		newc->outboundMsgs = ListInitialize();
		newc->inboundMsgs = ListInitialize();
		newc->queuedMsgs = ListInitialize();
		newc->registrations = ListInitialize();
		newc->will = NULL;
		newc->good = 1;
		newc->noLocal = 0; /* (connect->version == PRIVATE_PROTOCOL_VERSION) ? 1 : 0; */
		newc->discardedMsgs = 0;
		newc->connect_state = 0;
		newc->addr = malloc(strlen(clientAddr)+1);
		newc->pendingRegistration = NULL;
#if !defined(NO_BRIDGE)
		newc->pendingSubscription = NULL;
#endif
		strcpy(newc->addr,clientAddr);
		time( &(newc->lastContact) );
		ListAppend(bstate->clients, newc, sizeof(Clients) + strlen(newc->clientID)+1 + 3*sizeof(List) + strlen(newc->addr)+1);

		Log(LOG_PROTOCOL, 138, NULL, connect->clientID, sock);

		connect->clientID = NULL; /* don't want to free this space as it is being used in the clients list above */

		if (newc->cleansession)
			MQTTProtocol_removeAllSubscriptions(newc->clientID); /* clear any persistent subscriptions */

		if (connect->flags.will)
		{
			newc->connect_state = 1;
			rc = MQTTSPacket_send_willTopicReq(newc);
		}
		else
		{
			newc->connected = 1;
			rc = MQTTSPacket_send_connack(newc,0); /* send response */
		}
		client = newc;
	}
	else
	{
		client = (Clients*)(elem->content);
		if (client->connected)
		{
			Log(LOG_INFO, 34, NULL, connect->clientID);
			if (client->socket != sock)
				Socket_close(client->socket);
		}
		client->socket = sock;
		client->connected = 0; /* Do not connect until we know the connack has been sent */
		client->connect_state = 0;
		client->good = 1;
		if (client->addr != NULL)
			free(client->addr);
		client->addr = malloc(strlen(clientAddr)+1);
		strcpy(client->addr,clientAddr);

		client->cleansession = connect->flags.cleanSession;
		if (client->cleansession)
		{
			MQTTProtocol_removeAllSubscriptions(client->clientID);
			/* empty pending message lists */
			MQTTProtocol_emptyMessageList(client->outboundMsgs);
			MQTTProtocol_emptyMessageList(client->inboundMsgs);
			MQTTProtocol_emptyMessageList(client->queuedMsgs);
			MQTTProtocol_clearWill(client);
		}
		/* registrations are always cleared */
		MQTTSProtocol_emptyRegistrationList(client->registrations);

		client->keepAliveInterval = connect->keepAlive;
		client->pendingRegistration = NULL;
#if !defined(NO_BRIDGE)
		client->pendingSubscription = NULL;
#endif

		if (connect->flags.will)
		{
			client->connect_state = 1;
			rc = MQTTSPacket_send_willTopicReq(client);
		}
		else
		{
			client->connected = 1;
			rc = MQTTSPacket_send_connack(client,0); /* send response */
		}
	}
	if (existingClient)
		MQTTProtocol_processQueued(client);

	Log(LOG_INFO, 0, "Client connected to udp port %d from %s (%s)", list->port, client->clientID, clientAddr);

	MQTTSPacket_free_packet(pack);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleWillTopics(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	if (client->connect_state == 1)
	{
		MQTTS_WillTopic* willTopic = (MQTTS_WillTopic*)pack;
		MQTTProtocol_setWillTopic(client,willTopic->willTopic,willTopic->flags.retain, willTopic->flags.QoS);
		willTopic->willTopic = NULL;
		client->connect_state = 2;
		rc = MQTTSPacket_send_willMsgReq(client);
	}
	MQTTSPacket_free_packet(pack);
	time( &(client->lastContact) );
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleWillMsgs(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	if (client->connect_state == 2)
	{
		MQTTS_WillMsg* willMsg = (MQTTS_WillMsg*)pack;
		MQTTProtocol_setWillMsg(client, willMsg->willMsg);
		willMsg->willMsg = NULL;
		client->connect_state = 0;
		client->connected = 1;
		rc = MQTTSPacket_send_connack(client,0); /* send response */
	}
	MQTTSPacket_free_packet(pack);
	time( &(client->lastContact) );
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleRegisters(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_Register* registerPack = (MQTTS_Register*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	ListElement* elem = NULL;
	int topicId = 0;

	FUNC_ENTRY;
	if ((elem = ListFindItem(client->registrations, registerPack->topicName, registeredTopicNameCompare)) == NULL)
	{
		topicId = (MQTTSProtocol_registerTopic(client, registerPack->topicName))->id;
		registerPack->topicName = NULL;
	}
	else
		topicId = ((Registration*)(elem->content))->id;

	rc = MQTTSPacket_send_regAck(client,registerPack->msgId,topicId,MQTTS_RC_ACCEPTED );
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}
int MQTTSProtocol_handleRegacks(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	MQTTS_RegAck* regack = (MQTTS_RegAck*)pack;

	FUNC_ENTRY;
	if (client->pendingRegistration == NULL ||
			regack->msgId != client->pendingRegistration->msgId)
	{
		/* unexpected regack*/
	}
	else if (!client->outbound)
	{
		if (regack->topicId != client->pendingRegistration->registration->id)
		{
			/* unexpected regack*/
		}
		else if (regack->returnCode != MQTTS_RC_ACCEPTED)
		{
			/* rejected ack... what do we do now? */
		}
		else
		{
			free(client->pendingRegistration);
			client->pendingRegistration = NULL;
			rc = MQTTProtocol_processQueued(client);
		}
	}
	else /* outbound client */
	{
		if (regack->returnCode != MQTTS_RC_ACCEPTED)
		{
			/* rejected ack... what do we do now? */
		}
		else
		{
			Registration* reg = client->pendingRegistration->registration;
			free(client->pendingRegistration);
			client->pendingRegistration = NULL;
			reg->id = regack->topicId;
			ListAppend(client->registrations, reg, sizeof(reg) + strlen(reg->topicName)+1);
			rc = MQTTProtocol_processQueued(client);
		}

	}

	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handlePublishes(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_Publish* pub = (MQTTS_Publish*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	char* topicName = NULL;
	int msgId = 0;
#if !defined(NO_BRIDGE)
	char* bridgeTopic = NULL;
	char* originalTopic = NULL;
#endif

	FUNC_ENTRY;
	if (client != NULL && pub->topicId != 0) /*TODO: pre registered */
		topicName=MQTTSProtocol_getRegisteredTopicName(client,pub->topicId);
	else if (pub->shortTopic != NULL)
		topicName = pub->shortTopic;

	if (topicName == NULL)
	{
		/* TODO: unrecognized topic */
	}
	else
	{
		Publish* publish = malloc(sizeof(Publish));
		publish->header.bits.type = PUBLISH;
		publish->header.bits.qos = pub->flags.QoS;
		publish->header.bits.retain = pub->flags.retain;
		publish->header.bits.dup = pub->flags.dup;
		publish->msgId = pub->msgId;
		publish->payload = pub->data;
		publish->payloadlen = pub->dataLen;
		publish->topic = topicName;

#if !defined(NO_BRIDGE)
		if (client && client->outbound)
		{
			originalTopic = topicName;
			bridgeTopic = malloc(strlen(topicName)+1);
			strcpy(bridgeTopic, topicName);
			publish->topic = bridgeTopic;
			Bridge_handleInbound(client, publish);
			bridgeTopic = publish->topic;
		}
#endif


		if (pub->flags.QoS == 3)
		{
			publish->header.bits.qos = 0;
			Protocol_processPublication(publish, clientAddr);
			publish->topic = NULL;
		}
		else if (publish->header.bits.qos == 0)
		{
			/*
			 * TODO: msgs counts
			//if (strlen(publish->topic) < 5 || strncmp(publish->topic, sysprefix, strlen(sysprefix)) != 0)
			//	++(bstate.msgs_received);
			 */
			Protocol_processPublication(publish, client->clientID);
			publish->topic = NULL;
		}
		else if (publish->header.bits.qos == 1)
		{
			/* TODO: Log */
			rc = MQTTSPacket_send_puback(client,pub,MQTTS_RC_ACCEPTED);
			Protocol_processPublication(publish, client->clientID);
			publish->topic = NULL;
			/* TODO: msgs counts */
			/* ++(state.msgs_received); */
		}
		else if (publish->header.bits.qos == 2)
		{
			int len;
			Messages* m = malloc(sizeof(Messages));
			Publications* p = MQTTProtocol_storePublication(publish, &len);
			m->publish = p;
			m->msgid = publish->msgId;
			m->qos = publish->header.bits.qos;
			m->retain = publish->header.bits.retain;
			m->nextMessageType = PUBREL;
			msgId = publish->msgId;
			publish->topic = NULL;

			ListAppend(client->inboundMsgs, m, sizeof(Messages) + len);

			/* TODO: Log */
			rc = MQTTSPacket_send_pubrec(client,publish->msgId);
		}
		MQTTPacket_freePublish(publish);
	}
#if !defined(NO_BRIDGE)
	if (originalTopic != NULL && originalTopic != bridgeTopic)
		free(bridgeTopic);
#endif
	if (client != NULL)
		time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);

	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handlePubacks(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_PubAck* puback = (MQTTS_PubAck*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	/* look for the message by message id in the records of outbound messages for this client */
	if (ListFindItem(client->outboundMsgs, &(puback->msgId), messageIDCompare) == NULL)
		Log(LOG_WARNING, 50, NULL, "PUBACK", client->clientID, puback->msgId);
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 1)
			Log(LOG_WARNING, 51, NULL, "PUBACK", client->clientID, puback->msgId, m->qos);
		else
		{
			Log(TRACE_MAX, 4, NULL, client->clientID, puback->msgId);
			MQTTProtocol_removePublication(m->publish);
			ListRemove(client->outboundMsgs, m);
			/* TODO: msgs counts */
			/* (++state.msgs_sent);*/
		}
	}
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handlePubcomps(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_PubComp* pubcomp = (MQTTS_PubComp*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 19, NULL, pubcomp->msgId, client->clientID);

	/* look for the message by message id in the records of outbound messages for this client */
	if (ListFindItem(client->outboundMsgs, &(pubcomp->msgId), messageIDCompare) == NULL)
	{
		/* No Dupe flag in MQTTs
		if (pubcomp->header.dup == 0)
		*/
		Log(LOG_WARNING, 50, NULL, "PUBCOMP", client->clientID, pubcomp->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 2)
			Log(LOG_WARNING, 51, NULL, "PUBCOMP", client->clientID, pubcomp->msgId, m->qos);
		else
		{
			if (m->nextMessageType != PUBCOMP)
				Log(LOG_WARNING, 52, NULL, "PUBCOMP", client->clientID, pubcomp->msgId);
			else
			{
				Log(TRACE_MAX, 5, NULL, client->clientID, pubcomp->msgId);
				MQTTProtocol_removePublication(m->publish);
				ListRemove(client->outboundMsgs, m);
				/* TODO: msgs counts */
				/*(++state.msgs_sent); */
			}
		}
	}
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handlePubrecs(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_PubRec* pubrec = (MQTTS_PubRec*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 15, NULL, pubrec->msgId, client->clientID);

	/* look for the message by message id in the records of outbound messages for this client */
	client->outboundMsgs->current = NULL;
	if (ListFindItem(client->outboundMsgs, &(pubrec->msgId), messageIDCompare) == NULL)
	{
		/* No Dupe flag in MQTTs
		if (pubrec->header.dup == 0)
		*/
		Log(LOG_WARNING, 50, NULL, "PUBREC", client->clientID, pubrec->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 2)
		{
			/* No Dupe flag in MQTTs
			if (pubrec->header.dup == 0)
			*/
			Log(LOG_WARNING, 51, NULL, "PUBREC", client->clientID, pubrec->msgId, m->qos);
		}
		else if (m->nextMessageType != PUBREC)
		{
			/* No Dupe flag in MQTTs
			if (pubrec->header.dup == 0)
			*/
			Log(LOG_WARNING, 52, NULL, "PUBREC", client->clientID, pubrec->msgId);
		}
		else
		{
			Log(LOG_PROTOCOL, 16, NULL, client->clientID, pubrec->msgId);
			rc = MQTTSPacket_send_pubrel(client, pubrec->msgId);
			m->nextMessageType = PUBCOMP;
			time(&(m->lastTouch));
		}
	}
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handlePubrels(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_PubRel* pubrel = (MQTTS_PubRel*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	/* look for the message by message id in the records of inbound messages for this client */
	if (ListFindItem(client->inboundMsgs, &(pubrel->msgId), messageIDCompare) == NULL)
	{
		/* TODO: no dup flag in mqtts... not sure this is right
		if (pubrel->header.dup == 0)
			Log(LOG_WARNING, 50, "PUBREL", client->clientID, pubrel->msgId);
		else
		*/
			/* Apparently this is "normal" behaviour, so we don't need to issue a warning */
			rc = MQTTSPacket_send_pubcomp(client,pubrel->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->inboundMsgs->current->content);
		if (m->qos != 2)
			Log(LOG_WARNING, 51, NULL, "PUBREL", client->clientID, pubrel->msgId, m->qos);
		else if (m->nextMessageType != PUBREL)
			Log(LOG_WARNING, 52, NULL, "PUBREL", client->clientID, pubrel->msgId);
		else
		{
			Publish publish;

			/* send pubcomp before processing the publications because a lot of return publications could fill up the socket buffer */
			Log(LOG_PROTOCOL, 62, NULL, client->clientID, pubrel->msgId);
			rc = MQTTSPacket_send_pubcomp(client, pubrel->msgId);
			publish.header.bits.qos = m->qos;
			publish.header.bits.retain = m->retain;
			publish.msgId = m->msgid;
			publish.topic = m->publish->topic;
			publish.payload = m->publish->payload;
			publish.payloadlen = m->publish->payloadlen;
			Protocol_processPublication(&publish, client->clientID);
			MQTTProtocol_removePublication(m->publish);
			ListRemove(client->inboundMsgs, m);
			/* TODO: msgs counts */
			/* ++(state.msgs_received); */
		}
	}
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleSubscribes(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_Subscribe* sub = (MQTTS_Subscribe*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	int isnew;
	int topicId = 0;
	char* topicName = NULL;

	FUNC_ENTRY;
	if (sub->flags.topicIdType == MQTTS_TOPIC_TYPE_PREDEFINED)
	{
		topicName = MQTTSProtocol_getRegisteredTopicName(client,sub->topicId);
		topicId = sub->topicId;
	}
	else
	{
		topicName = sub->topicName;
		sub->topicName = NULL;
	}

	if (topicName == NULL)
		rc = MQTTSPacket_send_subAck(client, sub, 0, sub->flags.QoS, MQTTS_RC_REJECTED_INVALID_TOPIC_ID);
	else
	{
		if (sub->flags.topicIdType == MQTTS_TOPIC_TYPE_NORMAL && !Topics_hasWildcards(topicName))
		{
			char* regTopicName = malloc(strlen(topicName)+1);
			strcpy(regTopicName,topicName);
			topicId = (MQTTSProtocol_registerTopic(client, regTopicName))->id;
		}

		isnew = SubscriptionEngines_subscribe(bstate->se, client->clientID,
				topicName, sub->flags.QoS, client->noLocal, (client->cleansession == 0));

		if ( (rc = MQTTSPacket_send_subAck(client, sub, topicId, sub->flags.QoS, MQTTS_RC_ACCEPTED)) == 0)
			if ((client->noLocal == 0) || isnew)
				MQTTProtocol_processRetaineds(client, topicName,sub->flags.QoS);
	}
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleUnsubscribes(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	MQTTS_Unsubscribe* unsub = (MQTTS_Unsubscribe*)pack;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	char* topicName = NULL;

	FUNC_ENTRY;
	if (unsub->flags.topicIdType == 0x01)
		topicName=MQTTSProtocol_getRegisteredTopicName(client,unsub->topicId);
	else
		topicName = unsub->topicName;

	if (topicName != NULL)
		SubscriptionEngines_unsubscribe(bstate->se, client->clientID, topicName);

	rc = MQTTSPacket_send_unsubAck(client, unsub->msgId);
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleUnsubacks(void* pack, int sock, char* clientAddr){ return 0; }

int MQTTSProtocol_handlePingreqs(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	rc = MQTTSPacket_send_pingResp(client);
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}
int MQTTSProtocol_handlePingresps(void* pack, int sock, char* clientAddr)
{
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	client->ping_outstanding = 0;
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT;
	return 0;
}

int MQTTSProtocol_handleDisconnects(void* pack, int sock, char* clientAddr)
{
	Clients* client = Protocol_getclientbyaddr(clientAddr);

	FUNC_ENTRY;
	client->good = 0; /* don't try and send log message to this client if it is subscribed to $SYS/broker/log */
	Log((bstate->connection_messages) ? LOG_INFO : LOG_PROTOCOL, 38, NULL, client->clientID);
	MQTTProtocol_closeSession(client, 0);
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT;
	return 0;
}
int MQTTSProtocol_handleWillTopicUpds(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	MQTTS_WillTopicUpd* willTopic = (MQTTS_WillTopicUpd*)pack;

	FUNC_ENTRY;
	if (willTopic->willTopic != NULL)
	{
		MQTTProtocol_setWillTopic(client, willTopic->willTopic,willTopic->flags.retain, willTopic->flags.QoS);
		willTopic->willTopic = NULL;
	} else {
		MQTTProtocol_clearWill(client);
	}
	rc = MQTTSPacket_send_willTopicResp(client);
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_handleWillTopicResps(void* pack, int sock, char* clientAddr)
{
	return 0;
}

int MQTTSProtocol_handleWillMsgUpds(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	MQTTS_WillMsgUpd* willMsg = (MQTTS_WillMsgUpd*)pack;

	FUNC_ENTRY;
	MQTTProtocol_setWillMsg(client, willMsg->willMsg);
	willMsg->willMsg = NULL;
	rc = MQTTSPacket_send_willMsgResp(client);
	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}
int MQTTSProtocol_handleWillMsgResps(void* pack, int sock, char* clientAddr)
{
	return 0;
}


char* MQTTSProtocol_getRegisteredTopicName(Clients* client, int topicId)
{
	ListElement* elem;
	char* rc = NULL;

	FUNC_ENTRY;
	if ((elem = ListFindItem(client->registrations, &topicId, registeredTopicIdCompare)) == NULL)
		goto exit;
	if ( client->pendingRegistration!= NULL && elem->content == client->pendingRegistration->registration )
		goto exit;
	rc = ((Registration*)(elem->content))->topicName;
exit:
	FUNC_EXIT;
	return rc;
}

int MQTTSProtocol_getRegisteredTopicId(Clients* client, char* topicName)
{
	ListElement* elem;
	int rc = 0;

	FUNC_ENTRY;
	if ((elem = ListFindItem(client->registrations, topicName, registeredTopicNameCompare)) == NULL)
		goto exit;
	if ( client->pendingRegistration!= NULL && elem->content == client->pendingRegistration->registration )
		goto exit;
	rc = ((Registration*)(elem->content))->id;
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

void MQTTSProtocol_emptyRegistrationList(List* regList)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(regList, &current))
	{
		Registration* m = (Registration*)(current->content);
		free(m->topicName);
	}
	ListEmpty(regList);
	FUNC_EXIT;
}
void MQTTSProtocol_freeRegistrationList(List* regList)
{
	FUNC_ENTRY;
	MQTTSProtocol_emptyRegistrationList(regList);
	free(regList);
	FUNC_EXIT;
}

Registration* MQTTSProtocol_registerTopic(Clients* client, char* topicName)
{
	Registration* reg = malloc(sizeof(Registration));

	FUNC_ENTRY;
	reg->topicName = topicName;
	reg->id = client->registrations->count+1;
	ListAppend(client->registrations, reg, sizeof(reg) + strlen(reg->topicName)+1);
	FUNC_EXIT;
	return reg;
}

int MQTTSProtocol_startRegistration(Clients* client, char* topic)
{
	int rc = 0;

	FUNC_ENTRY;
	if (client->outbound)
		rc = MQTTSProtocol_startClientRegistration(client,topic);
	else
	{
		PendingRegistration* pendingReg = malloc(sizeof(PendingRegistration));
		Registration* reg;
		int msgId = MQTTProtocol_assignMsgId(client);
		char* regTopicName = malloc(strlen(topic)+1);
		strcpy(regTopicName,topic);
		reg = MQTTSProtocol_registerTopic(client, regTopicName);
		pendingReg->msgId = msgId;
		pendingReg->registration = reg;
		time(&(pendingReg->sent));
		client->pendingRegistration = pendingReg;
		rc = MQTTSPacket_send_register(client, reg->id, regTopicName, msgId);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_startPublishCommon(Clients* client, Publish* mqttPublish, int dupe, int qos, int retained)
{
	int rc = 0;
	int topicId = 0;
	MQTTS_Publish* pub = malloc(sizeof(MQTTS_Publish));

	FUNC_ENTRY;
	if (strlen(mqttPublish->topic) > 2 &&
			(topicId=MQTTSProtocol_getRegisteredTopicId(client,mqttPublish->topic))==0)
	{
		/* TODO: Logic elsewhere _should_ mean this case never happens... */
		/*printf("I want to send a msg to %s on topic %s but it isn't registered\n",client->clientID,mqttPublish->topic); */
	}
	else
	{
		pub->header.type = MQTTS_PUBLISH;
		pub->flags.QoS = qos;
		pub->flags.retain = retained;
		pub->flags.dup = dupe;
		pub->msgId = mqttPublish->msgId;
		pub->data = mqttPublish->payload;
		pub->shortTopic = NULL;
		if (mqttPublish->payloadlen > 248)
		{
			/* TODO: add a message for truncated payload */
			/* printf("Truncating a %d byte message sent to %s on topic %s\n",mqttPublish->payloadlen, client->clientID, mqttPublish->topic);*/
			pub->dataLen = 248;
		}
		else
			pub->dataLen = mqttPublish->payloadlen;

		pub->header.len = 7+pub->dataLen;

		if (strlen(mqttPublish->topic) < 3)
		{
			pub->flags.topicIdType = MQTTS_TOPIC_TYPE_SHORT;
			pub->shortTopic = mqttPublish->topic;
		}
		else
			pub->topicId = topicId;

		rc = MQTTSPacket_send_publish(client, pub);

		pub->data = NULL;
		pub->shortTopic = NULL;
	}
	MQTTSPacket_free_packet((MQTTS_Header*)pub);
	FUNC_EXIT_RC(rc);
	return rc;
}


#endif /* #if defined(MQTTS */
