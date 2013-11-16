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
 * MQTT Protocol module - those functions needed by the MQTT client.
 * Some other related functions are in the MQTTPacketOut module
 */

#include <stdlib.h>

#include "MQTTProtocolClient.h"
#if defined(MQTTS)
#include "MQTTSProtocol.h"
#endif
#include "Protocol.h"
#include "SocketBuffer.h"
#include "StackTrace.h"
#include "Heap.h"

#if defined(MQTTS)
Registration* MQTTSProtocol_registerTopic(Clients* client, char* topicName);
#endif


extern MQTTProtocol state;		/**< MQTT Protocol state shared with the MQTTProtocol module */
extern BrokerStates* bstate; 	/**< broker state shared with the MQTTProtocol module */
extern int in_MQTTPacket_Factory;	/**< flag shared with the MQTTProtocol module */

void MQTTProtocol_removePublication(Publications* p);

/**
 * List callback function for comparing Message structures by message id
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int messageIDCompare(void* a, void* b)
{
	Messages* msg = (Messages*)a;
	return msg->msgid == *(int*)b;
}


/**
 * Assign a new message id for a client.  Make sure it isn't already being used and does
 * not exceed the maximum.
 * @param client a client structure
 * @return the next message id to use
 */
int MQTTProtocol_assignMsgId(Clients* client)
{
	FUNC_ENTRY;
	++(client->msgID);
	while (ListFindItem(client->outboundMsgs, &(client->msgID), messageIDCompare) != NULL)
		++(client->msgID);
	if (client->msgID == MAX_MSG_ID)
		client->msgID = 1;
	FUNC_EXIT_RC(client->msgID);
	return client->msgID;
}


void MQTTProtocol_storeQoS0(Clients* pubclient, Publish* publish)
{
	int len;
	pending_write* pw = NULL;

	FUNC_ENTRY;
	/* store the publication until the write is finished */
	pw = malloc(sizeof(pending_write));
	Log(TRACE_MIN, 37, NULL);
	pw->p = MQTTProtocol_storePublication(publish, &len);
	pw->socket = pubclient->socket;
	pw->client = pubclient;
	ListAppend(&(state.pending_writes), pw, sizeof(pending_write)+len);
	/* we don't copy QoS 0 messages unless we have to, so now we have to tell the socket buffer where
	the saved copy is */
	if (SocketBuffer_updateWrite(pw->socket, pw->p->topic, pw->p->payload) == NULL)
		Log(LOG_SEVERE, 0, "Error updating write");
	FUNC_EXIT;
}


/**
 * Utility function to start a new publish exchange.
 * @param pubclient the client to send the publication to
 * @param publish the publication data
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @return the completion code
 */
int MQTTProtocol_startPublishCommon(Clients* pubclient, Publish* publish, int qos, int retained)
{
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	if (qos == 0 && (strlen(publish->topic) < 5 || strncmp(publish->topic, sysprefix, strlen(sysprefix)) != 0))
	{
		++(bstate->msgs_sent);
		bstate->bytes_sent += publish->payloadlen;
	}
#if defined(MQTTS)
	if (pubclient->protocol == PROTOCOL_MQTTS)
	{
		rc = MQTTSProtocol_startPublishCommon(pubclient,publish,0,qos,retained);
	}
	else
	{
#endif
	rc = MQTTPacket_send_publish(publish, 0, qos, retained, pubclient->socket, pubclient->clientID);
	if (qos == 0 && rc == TCPSOCKET_INTERRUPTED)
		MQTTProtocol_storeQoS0(pubclient, publish);
#if defined(MQTTS)
	}
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Start a new publish exchange.  Store any state necessary and try to send the packet
 * @param pubclient the client to send the publication to
 * @param publish the publication data
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @param mm - pointer to the message to send
 * @return the completion code
 */
int MQTTProtocol_startPublish(Clients* pubclient, Publish* publish, int qos, int retained, Messages** mm)
{
	Publish p = *publish;
	int rc = 0;

	FUNC_ENTRY;
	if (qos > 0)
	{
		p.msgId = publish->msgId = MQTTProtocol_assignMsgId(pubclient);
		*mm = MQTTProtocol_createMessage(publish, mm, qos, retained);
		ListAppend(pubclient->outboundMsgs, *mm, (*mm)->len);
		/* we change these pointers to the saved message location just in case the packet could not be written
		entirely; the socket buffer will use these locations to finish writing the packet */
		p.payload = (*mm)->publish->payload;
		p.topic = (*mm)->publish->topic;
	}
	rc = MQTTProtocol_startPublishCommon(pubclient, &p, qos, retained);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Start a new publication exchange for a previously queued message.
 * @param pubclient the client to send the publication to
 * @param m pointer to the message to send
 * @return the completion code
 */
int MQTTProtocol_startQueuedPublish(Clients* pubclient, Messages* m)
{
	int rc = 0;
	Publish publish;

	FUNC_ENTRY;
	if (m->qos > 0)
	{
		m->msgid = MQTTProtocol_assignMsgId(pubclient);
		ListAppend(pubclient->outboundMsgs, m, m->len);
	}
	publish.header.byte = 0;
	publish.header.bits.qos = m->qos;
	publish.header.bits.retain = m->retain;
	publish.header.bits.type = PUBLISH;
	publish.msgId = m->msgid;
	publish.payload = m->publish->payload;
	publish.payloadlen = m->publish->payloadlen;
	publish.topic = m->publish->topic;
	rc = MQTTProtocol_startPublishCommon(pubclient, &publish, m->qos, m->retain);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Add a new publication to a client outbound message queue.
 * @param pubclient the client to send the publication to
 * @param publish the publication data
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @param mm - pointer to the message to send
 * @return the completion code
 */
int MQTTProtocol_queuePublish(Clients* pubclient, Publish* publish, int qos, int retained, int priority, Messages** mm)
{
	int rc = TCPSOCKET_COMPLETE;
#define THRESHOLD 80

	FUNC_ENTRY;

	/* if qos == 0 then add this client to the list of clients with queued QoS 0 */

	Log(TRACE_MAXIMUM, 3, NULL, pubclient->clientID, qos);
	if (queuedMsgsCount(pubclient) < bstate->max_queued_messages)
	{
		int threshold = (THRESHOLD * bstate->max_queued_messages) / 100;
		*mm = MQTTProtocol_createMessage(publish, mm, qos, retained);
		if (priority < PRIORITY_LOW || priority > PRIORITY_HIGH)
		{
			Log(LOG_ERROR, 13, "Priority %d reassigned to normal", priority);
			priority = PRIORITY_NORMAL;
		}
		ListAppend(pubclient->queuedMsgs[priority], *mm, (*mm)->len);
		if (queuedMsgsCount(pubclient) == threshold + 1)
			Log(LOG_WARNING, 145, NULL, pubclient->clientID, THRESHOLD);
	}
	else
	{
		if (++(pubclient->discardedMsgs) > bstate->max_queued_messages * 10)
			rc = SOCKET_ERROR;
		if ((pubclient->discardedMsgs == 1) || (pubclient->discardedMsgs == 10) || (pubclient->discardedMsgs % 100 == 0))
			Log(LOG_WARNING, 45, NULL, pubclient->clientID, pubclient->discardedMsgs);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Copy and store message data for retries
 * @param publish the publication data
 * @param mm - pointer to the message data to store
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @return pointer to the message data stored
 */
Messages* MQTTProtocol_createMessage(Publish* publish, Messages **mm, int qos, int retained)
{
	Messages* m = malloc(sizeof(Messages));

	FUNC_ENTRY;
	m->len = sizeof(Messages);
	if (*mm == NULL || (*mm)->publish == NULL)
	{
		int len1;
		*mm = m;
		m->publish = MQTTProtocol_storePublication(publish, &len1);
		m->len += len1;
	}
	else
	{
		++(((*mm)->publish)->refcount);
		m->publish = (*mm)->publish;
	}
	m->msgid = publish->msgId;
	m->qos = qos;
	m->retain = retained;
	time(&(m->lastTouch));
	if (qos == 2)
		m->nextMessageType = PUBREC;
	FUNC_EXIT;
	return m;
}


/**
 * Store message data for possible retry
 * @param publish the publication data
 * @param len returned length of the data stored
 * @return the publication stored
 */
Publications* MQTTProtocol_storePublication(Publish* publish, int* len)
{ /* store publication for possible retry */
	Publications* p = malloc(sizeof(Publications));

	FUNC_ENTRY;
	p->refcount = 1;

	*len = strlen(publish->topic)+1;
	p->topic = malloc(*len);
	strcpy(p->topic, publish->topic);
	*len += sizeof(Publications);

	p->payloadlen = publish->payloadlen;
	p->payload = malloc(publish->payloadlen);
	memcpy(p->payload, publish->payload, p->payloadlen);
	*len += publish->payloadlen;

	ListAppend(&(state.publications), p, *len);
	FUNC_EXIT;
	return p;
}


/**
 * Process an incoming publish packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePublishes(void* pack, int sock, Clients* client)
{
	Publish* publish = (Publish*)pack;
	char* clientid = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	if (client == NULL)
		clientid = INTERNAL_CLIENTID; /* this is an internal client */
	else
	{
		clientid = client->clientID;
		Log(LOG_PROTOCOL, 11, NULL, sock, clientid, publish->msgId, publish->header.bits.qos,
				publish->header.bits.retain);
	}
	rc = Protocol_handlePublishes(publish, sock, client, clientid);

	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Remove stored message data.  Opposite of storePublication
 * @param p stored publication to remove
 */
void MQTTProtocol_removePublication(Publications* p)
{
	FUNC_ENTRY;
	if (p == NULL)
		Log(LOG_ERROR, 13, "Publication pointer is null - cannot free");
	else if (--(p->refcount) == 0)
	{
		free(p->payload);
		free(p->topic);
		ListRemove(&(state.publications), p);
	}
	FUNC_EXIT;
}


/**
 * Process an incoming puback packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubacks(void* pack, int sock, Clients* client)
{
	Puback* puback = (Puback*)pack;
	//Clients* client = (Clients*)(TreeFind(bstate->clients, &sock)->content);
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 14, NULL, sock, client->clientID, puback->msgId);

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
			Log(TRACE_MIN, 4, NULL, client->clientID, puback->msgId);
			++(bstate->msgs_sent);
			bstate->bytes_sent += m->publish->payloadlen;
			MQTTProtocol_removePublication(m->publish);
			ListRemove(client->outboundMsgs, m);
			/* now there is space in the inflight message queue we can process any queued messages */
			MQTTProtocol_processQueued(client);
		}
	}
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubrec packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubrecs(void* pack, int sock, Clients* client)
{
	Pubrec* pubrec = (Pubrec*)pack;
	//Clients* client = (Clients*)(TreeFind(bstate->clients, &sock)->content);
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 15, NULL, sock, client->clientID, pubrec->msgId);

	/* look for the message by message id in the records of outbound messages for this client */
	client->outboundMsgs->current = NULL;
	if (ListFindItem(client->outboundMsgs, &(pubrec->msgId), messageIDCompare) == NULL)
	{
		if (pubrec->header.bits.dup == 0)
			Log(LOG_WARNING, 50, NULL, "PUBREC", client->clientID, pubrec->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 2)
		{
			if (pubrec->header.bits.dup == 0)
				Log(LOG_WARNING, 51, NULL, "PUBREC", client->clientID, pubrec->msgId, m->qos);
		}
		else if (m->nextMessageType != PUBREC)
		{
			if (pubrec->header.bits.dup == 0)
				Log(LOG_WARNING, 52, NULL, "PUBREC", client->clientID, pubrec->msgId);
		}
		else
		{
			rc = MQTTPacket_send_pubrel(pubrec->msgId, 0, sock, client->clientID);
			m->nextMessageType = PUBCOMP;
			time(&(m->lastTouch));
		}
	}
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubrel packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubrels(void* pack, int sock, Clients* client)
{
	Pubrel* pubrel = (Pubrel*)pack;
	//Clients* client = (Clients*)(TreeFind(bstate->clients, &sock)->content);
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 17, NULL, sock, client->clientID, pubrel->msgId);

	/* look for the message by message id in the records of inbound messages for this client */
	if (ListFindItem(client->inboundMsgs, &(pubrel->msgId), messageIDCompare) == NULL)
	{
		if (pubrel->header.bits.dup == 0)
			Log(LOG_WARNING, 50, NULL, "PUBREL", client->clientID, pubrel->msgId);
		/* Apparently this is "normal" behaviour, so we don't need to issue a warning */
		rc = MQTTPacket_send_pubcomp(pubrel->msgId, sock, client->clientID);
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
			char* saved_clientid = malloc(strlen(client->clientID) + 1);

			strcpy(saved_clientid, client->clientID);
			/* send pubcomp before processing the publications because a lot of return publications could fill up the socket buffer */
			rc = MQTTPacket_send_pubcomp(pubrel->msgId, sock, client->clientID);
			publish.header.bits.qos = m->qos;
			publish.header.bits.retain = m->retain;
			publish.msgId = m->msgid;
			publish.topic = m->publish->topic;
			publish.payload = m->publish->payload;
			publish.payloadlen = m->publish->payloadlen;
			++(bstate->msgs_received);
			bstate->bytes_received += m->publish->payloadlen;
			Protocol_processPublication(&publish, client->clientID);

			/* The client structure might have been removed in processPublication, on error */
			if (TreeFind(bstate->clients, &sock) || TreeFind(bstate->disconnected_clients, saved_clientid))
			{
				ListRemove(client->inboundMsgs, m);
				MQTTProtocol_removePublication(m->publish);
			}
			free(saved_clientid);
		}
	}
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubcomp packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubcomps(void* pack, int sock, Clients* client)
{
	Pubcomp* pubcomp = (Pubcomp*)pack;
	//Clients* client = (Clients*)(TreeFind(bstate->clients, &sock)->content);
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 19, NULL, sock, client->clientID, pubcomp->msgId);

	/* look for the message by message id in the records of outbound messages for this client */
	if (ListFindItem(client->outboundMsgs, &(pubcomp->msgId), messageIDCompare) == NULL)
	{
		if (pubcomp->header.bits.dup == 0)
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
				Log(TRACE_MIN, 5, NULL, client->clientID, pubcomp->msgId);
				++(bstate->msgs_sent);
				bstate->bytes_sent += m->publish->payloadlen;
				MQTTProtocol_removePublication(m->publish);
				ListRemove(client->outboundMsgs, m);
				/* now there is space in the inflight message queue we can process any queued messages */
				MQTTProtocol_processQueued(client);
			}
		}
	}
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * MQTT protocol keepAlive processing.  Sends PINGREQ packets as required.
 * @param now current time
 */
void MQTTProtocol_keepalive(time_t now)
{
	Node* current = NULL;

	FUNC_ENTRY;
	current = TreeNextElement(bstate->clients, current);
	while (current)
	{
		Clients* client =	(Clients*)(current->content);
		current = TreeNextElement(bstate->clients, current);
#if !defined(NO_BRIDGE)
		if (client->outbound)
		{
			if (client->connected && client->keepAliveInterval > 0
					&& (difftime(now, client->lastContact) >= client->keepAliveInterval))
			{
				if (client->ping_outstanding)
				{
					Log(LOG_INFO, 143, NULL, client->keepAliveInterval, client->clientID);
					MQTTProtocol_closeSession(client, 1);
				}
				else
				{
#if defined(MQTTS)
					if (client->protocol == PROTOCOL_MQTTS)
					{
						int rc = MQTTSPacket_send_pingReq(client);
						if (rc == SOCKET_ERROR)
							MQTTProtocol_closeSession(client, 1);
					}
					else
#endif
						MQTTPacket_send_pingreq(client->socket, client->clientID);
					client->lastContact = now;
					client->ping_outstanding = 1;
				}
			}
		}
		else
#endif
		if (client->connected && client->keepAliveInterval > 0
					&& (difftime(now, client->lastContact) > 2*(client->keepAliveInterval)))
		{ /* zero keepalive interval means never disconnect */
			Log(LOG_INFO, 24, NULL, client->keepAliveInterval, client->clientID);
			MQTTProtocol_closeSession(client, 1);
		}
	}
	FUNC_EXIT;
}


/**
 * Start the publish exchange for any queued messages, if possible.
 * When the inflight message queue is not at maximum capacity we can start a new
 * publication.
 * @param client the client to process queued messages for
 */
int MQTTProtocol_processQueued(Clients* client)
{
	int rc = 0;
#if defined(QOS0_SEND_LIMIT)
	int qos0count = 0;
#endif
	int threshold_log_message_issued = 0;

	FUNC_ENTRY;
	if (Protocol_isClientQuiescing(client))
		goto exit; /* don't create new work - just finish in-flight stuff */

	Log(TRACE_MAXIMUM, 0, NULL, client->clientID);
	while (client->good && Socket_noPendingWrites(client->socket) && /* no point in starting a publish if a write is still pending */
		client->outboundMsgs->count < bstate->max_inflight_messages &&
		queuedMsgsCount(client) > 0
#if defined(QOS0_SEND_LIMIT) 
		&& qos0count < bstate->max_inflight_messages /* an arbitrary criterion - but when would we restart? */
#endif 
		#if defined(MQTTS)
		&& (client->protocol == PROTOCOL_MQTT || client->outboundMsgs->count == 0)
#endif
		)
	{
		int pubrc = TCPSOCKET_COMPLETE;
		Messages* m = NULL;
		int threshold = (THRESHOLD * bstate->max_queued_messages) / 100;
		List* queue = NULL;
		int i;

		for (i = PRIORITY_MAX-1; i >= 0; --i)
		{
			if (client->queuedMsgs[i]->count > 0)
			{
				queue = client->queuedMsgs[i];
				break;
			}
		}
		m = (Messages*)(queue->first->content);

		Log(TRACE_MAXIMUM, 1, NULL, client->clientID);
#if defined(MQTTS)
		if (client->protocol == PROTOCOL_MQTTS && strlen(m->publish->topic) > 2 &&
				MQTTSProtocol_getRegisteredTopicId(client, m->publish->topic) == 0)
		{
			if (client->pendingRegistration == NULL)
				rc = MQTTSProtocol_startRegistration(client, m->publish->topic);
				goto exit;
		}

#endif
#if defined(QOS0_SEND_LIMIT)
		if (m->qos == 0)
			++qos0count; 
#endif

		pubrc = MQTTProtocol_startQueuedPublish(client, m);
		/* regardless of whether the publish packet was sent on the wire (pubrc is good), the
		 * message has been put onto the outbound queue, so it must be removed from
		 * the queuedMsgs queue
		 */
		if (pubrc != TCPSOCKET_COMPLETE && pubrc != TCPSOCKET_INTERRUPTED)
			client->good = 0;
		if (m->qos == 0)
		{
			/* This is done primarily for MQTT-S.
			 * A qos-0 message will be on this queue if its topic
			 * has to be registered first. Now that the message
			 * has been sent, it needs to be cleaned up as there
			 * won't be an ack to trigger it.
			 *
			 * For MQTT, there is a scenario in which qos-0 messages
			 * could be on this list for which the same applies.
			 *
			 * Note (IGC): this is also a bug fix I just implemented - applies equally to MQTTs and MQTT!
			 */
			MQTTProtocol_removePublication(m->publish);
			if (!ListRemove(queue, m))
				Log(LOG_ERROR, 38, NULL);
		}
		else if (!ListDetach(queue, m))
			Log(LOG_ERROR, 38, NULL);
		if (queuedMsgsCount(client) == threshold - 1 && !threshold_log_message_issued)
		{
			Log(LOG_INFO, 146, NULL, client->clientID, THRESHOLD);
			threshold_log_message_issued = 1;
		}
	}
#if defined(QOS0_SEND_LIMIT)
	if (qos0count >= bstate->max_inflight_messages)
		rc = 1;
#endif
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * MQTT retry processing per client
 * @param now current time
 * @param client - the client to which to apply the retry processing
 */
void MQTTProtocol_retries(time_t now, Clients* client)
{
	ListElement* outcurrent = NULL;

	FUNC_ENTRY;
#if defined(MQTTS)
	if (client->protocol == PROTOCOL_MQTTS)
	{
		if (client->pendingRegistration != NULL &&
				difftime(now, client->pendingRegistration->sent) > bstate->retry_interval)
		{
			Registration* reg = client->pendingRegistration->registration;
			time(&client->pendingRegistration->sent);
			/* NB: no dup bit for these packets */
			if (MQTTSPacket_send_register(client, reg->id, reg->topicName, client->pendingRegistration->msgId) == SOCKET_ERROR)
			{
				client->good = 0;
				//TODO: update message
				Log(LOG_WARNING, 29, NULL, client->clientID, client->socket);
				MQTTProtocol_closeSession(client, 1);
				client = NULL;
			}
		}
#if !defined(NO_BRIDGE)
		if (client->pendingSubscription != NULL &&
				difftime(now, client->pendingSubscription->sent) > bstate->retry_interval)
		{
			time(&client->pendingSubscription->sent);
			if (MQTTSPacket_send_subscribe(client, client->pendingSubscription->topicName,client->pendingSubscription->qos, client->pendingSubscription->msgId) == SOCKET_ERROR)
			{
				client->good = 0;
				//TODO: update message
				Log(LOG_WARNING, 29, NULL, client->clientID, client->socket);
				MQTTProtocol_closeSession(client, 1);
				client = NULL;
			}
		}
#endif
	}
#endif
	while (client && ListNextElement(client->outboundMsgs, &outcurrent) &&
		   client->connected && client->good &&           /* client is connected and has no errors */
			Socket_noPendingWrites(client->socket))  /* there aren't any previous packets still stacked up on the socket */
	{
		Messages* m = (Messages*)(outcurrent->content);


		if (difftime(now, m->lastTouch) > bstate->retry_interval)
		{
			if (m->qos == 1 || (m->qos == 2 && m->nextMessageType == PUBREC))
			{
				Publish publish;
				int rc;

				Log(LOG_INFO, 28, NULL, client->clientID, client->socket, m->msgid);
				publish.msgId = m->msgid;
				publish.topic = m->publish->topic;
				publish.payload = m->publish->payload;
				publish.payloadlen = m->publish->payloadlen;
#if defined(MQTTS)
				if (client->protocol == PROTOCOL_MQTTS)
				{
					if (MQTTSProtocol_startPublishCommon(client, &publish, 1, m->qos, m->retain) == SOCKET_ERROR)
					{
						client->good = 0;
						Log(LOG_WARNING, 29, NULL, client->clientID, client->socket);
						MQTTProtocol_closeSession(client, 1);
						client = NULL;
					}
					else
						time(&(m->lastTouch));
				}
				else
				{
#endif
					rc = MQTTPacket_send_publish(&publish, 1, m->qos, m->retain, client->socket, client->clientID);
					if (rc == SOCKET_ERROR)
					{
						client->good = 0;
						Log(LOG_WARNING, 29, NULL, client->clientID, client->socket);
						MQTTProtocol_closeSession(client, 1);
						client = NULL;
					}
					else
					{
						if (m->qos == 0 && rc == TCPSOCKET_INTERRUPTED)
							MQTTProtocol_storeQoS0(client, &publish);
						time(&(m->lastTouch));
					}
#if defined(MQTTS)
				}
#endif
			}
			else if (m->qos && m->nextMessageType == PUBCOMP)
			{
				Log(LOG_WARNING, 30, NULL, client->clientID, m->msgid);
#if defined(MQTTS)
				if (client->protocol == PROTOCOL_MQTTS)
				{
					/* NB: no dup bit for PUBREL */
					if (MQTTSPacket_send_pubrel(client, m->msgid) == SOCKET_ERROR)
					{
						client->good = 0;
						Log(LOG_WARNING, 18, NULL, client->clientID, client->socket,
								Socket_getpeer(client->socket));
						MQTTProtocol_closeSession(client, 1);
						client = NULL;
					}
					else
						time(&(m->lastTouch));
				}
				else
#endif
					if (MQTTPacket_send_pubrel(m->msgid, 1, client->socket, client->clientID) != TCPSOCKET_COMPLETE)
					{
						client->good = 0;
						Log(LOG_WARNING, 18, NULL, client->clientID, client->socket,
								Socket_getpeer(client->socket));
						MQTTProtocol_closeSession(client, 1);
						client = NULL;
					}
					else
						time(&(m->lastTouch));
			}
			/* break; why not do all retries at once? */
		}
	}
	FUNC_EXIT;
}


/**
 * MQTT retry protocol and socket pending writes processing.
 * @param now current time
 * @param doRetry boolean - retries as well as pending writes?
 * @return not actually used
 */
int MQTTProtocol_retry(time_t now, int doRetry)
{
	Node* current = NULL;
	int rc = 0;

	FUNC_ENTRY;
	current = TreeNextElement(bstate->clients, current);
	/* look through the outbound message list of each client, checking to see if a retry is necessary */
	while (current)
	{
		Clients* client = (Clients*)(current->content);
		current = TreeNextElement(bstate->clients, current);
		if (client->connected == 0)
		{
#if defined(MQTTS)
			if (client->protocol == PROTOCOL_MQTTS)
			{
				if (difftime(now,client->lastContact) > bstate->retry_interval)
				{
					int rc2 = 0;
					/* NB: no dup bit for these packets */
					if (client->connect_state == 1) /* TODO: handle err */
						rc2 = MQTTSPacket_send_willTopicReq(client);
					else if (client->connect_state == 2) /* TODO: handle err */
						rc2 = MQTTSPacket_send_willMsgReq(client);
					if (rc2 == SOCKET_ERROR)
					{
						client->good = 0;
						Log(LOG_WARNING, 29, NULL, client->clientID, client->socket);
						MQTTProtocol_closeSession(client, 1);
						client = NULL;
					}
				}
			}
#endif
			continue;
		}
		if (client->good == 0)
		{
			MQTTProtocol_closeSession(client, 1);
			continue;
		}
		if (Socket_noPendingWrites(client->socket) == 0)
			continue;
		if (doRetry)
			MQTTProtocol_retries(now, client);
		if (client)
		{
			if (MQTTProtocol_processQueued(client))
				rc = 1;
		}
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free a client structure
 * @param client the client data to free
 */
void MQTTProtocol_freeClient(Clients* client)
{
	int i;

	FUNC_ENTRY;
	MQTTProtocol_removeAllSubscriptions(client->clientID);
	/* free up pending message lists here, and any other allocated data */
	MQTTProtocol_freeMessageList(client->outboundMsgs);
	MQTTProtocol_freeMessageList(client->inboundMsgs);
	if (queuedMsgsCount(client) > 0)
		Log(LOG_WARNING, 64, NULL, queuedMsgsCount(client), client->clientID);
	for (i = 0; i < PRIORITY_MAX; ++i)
		MQTTProtocol_freeMessageList(client->queuedMsgs[i]);
#if defined(MQTTS)
	if (client->registrations != NULL)
		MQTTSProtocol_freeRegistrationList(client->registrations);
	if (client->pendingRegistration != NULL)
		free(client->pendingRegistration);
#if !defined(NO_BRIDGE)
	if (client->pendingSubscription != NULL)
	{
		if (client->pendingSubscription->topicName)
			free(client->pendingSubscription->topicName);
		free(client->pendingSubscription);
	}
#endif
#endif
	if (client->addr != NULL)
		free(client->addr);
	free(client->clientID);
	if (client->will != NULL)
	{
		willMessages* w = client->will;
		free(w->msg);
		free(w->topic);
		free(client->will);
	}
	free(client);
	FUNC_EXIT;
}


/**
 * Remove any QoS 0 messages from a message list.
 * This is used to clean up a session for a client which is non-cleansession.  QoS 0 messages
 * are non-persistent, so they are removed from the queue.
 * @param msgList the message list to empty
 */
void MQTTProtocol_removeQoS0Messages(List* msgList)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	ListNextElement(msgList, &current);
	while (current)
	{
		Messages* m = (Messages*)(current->content);
		if (m->qos == 0)
		{
			MQTTProtocol_removePublication(m->publish);
			msgList->current = current;
			ListRemove(msgList, current->content);
			current = msgList->current;
		}
		else
			ListNextElement(msgList, &current);
	}
	FUNC_EXIT;
}


/**
 * Empty a message list, leaving it able to accept new messages
 * @param msgList the message list to empty
 */
void MQTTProtocol_emptyMessageList(List* msgList)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(msgList, &current))
	{
		Messages* m = (Messages*)(current->content);
		if (m)
			MQTTProtocol_removePublication(m->publish);
	}
	ListEmpty(msgList);
	FUNC_EXIT;
}


/**
 * Empty and free up all storage used by a message list
 * @param msgList the message list to empty and free
 */
void MQTTProtocol_freeMessageList(List* msgList)
{
	FUNC_ENTRY;
	MQTTProtocol_emptyMessageList(msgList);
	ListFree(msgList);
	FUNC_EXIT;
}

