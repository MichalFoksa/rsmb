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

#if !defined(NO_BRIDGE) && defined(MQTTS)


#include <stdlib.h>

#include "MQTTSProtocolOut.h"
#include "Clients.h"
#include "Bridge.h"
#include "Protocol.h"
#include "MQTTProtocolClient.h"
#include "StackTrace.h"

#include "Heap.h"

extern BrokerStates* bstate;

char* MQTTProtocol_addressPort(char* ip_address, int* port);
void Bridge_subscribe(BridgeConnections* bc, Clients* client);


Clients* MQTTSProtocol_connect(char* ip_address, char* clientID, int cleansession, int try_private, int keepalive, willMessages* willMessage)
{ /* outgoing connection */
	int rc, port;
	char* addr;
	Clients* newc = malloc(sizeof(Clients));

	FUNC_ENTRY;
	newc->clientID = clientID;
	newc->cleansession = cleansession;
	newc->outbound = 1;
	newc->connected = 0;
	newc->ping_outstanding = 0;
	newc->keepAliveInterval = keepalive;
	newc->msgID = 0;
	newc->outboundMsgs = ListInitialize();
	newc->inboundMsgs = ListInitialize();
	newc->queuedMsgs = ListInitialize();
	newc->registrations = ListInitialize();
	newc->will = willMessage;
	newc->good = 1;
	newc->connect_state = 0;
	newc->noLocal = try_private; /* try private connection first */
	time(&(newc->lastContact));
	newc->discardedMsgs = 0;
	newc->pendingRegistration = NULL;
	newc->protocol = PROTOCOL_MQTTS;

	addr = MQTTProtocol_addressPort(ip_address, &port);

	newc->addr = malloc(strlen(ip_address));
	strcpy(newc->addr,ip_address);

	rc = Socket_new_type(addr, port, &(newc->socket), SOCK_DGRAM);

	if (rc == EINPROGRESS || rc == EWOULDBLOCK)
		newc->connect_state = 1; // UDP connect called - improbable, but possible on obscure platforms
	else if (rc == 0)
	{
		Log(LOG_PROTOCOL, 69, NULL, newc->clientID, newc->socket, newc->noLocal);
		newc->connect_state = 2;
		rc = MQTTSPacket_send_connect(newc);
	}
	ListAppend(bstate->clients, newc, sizeof(Clients) + strlen(newc->clientID)+1 + 3*sizeof(List));

	FUNC_EXIT;
	return newc;
}

void MQTTSProtocol_reconnect(char* ip_address, Clients* client)
{
	int port, rc;
	char *address = MQTTProtocol_addressPort(ip_address, &port);

	FUNC_ENTRY;
	client->good = 1;
	client->ping_outstanding = 0;
	client->connect_state = 0;
	client->connected = 0;
	if (client->cleansession)
		client->msgID = 0;

	if (client->addr)
		free(client->addr);

	client->addr = malloc(strlen(ip_address));
	strcpy(client->addr,ip_address);

	rc = Socket_new_type(address, port, &(client->socket), SOCK_DGRAM);
	if (rc == EINPROGRESS || rc == EWOULDBLOCK)
		client->connect_state = 1; // TCP connect called
	else if (rc == 0)
	{
		Log(LOG_PROTOCOL, 68, NULL, client->clientID, client->socket);
		client->connect_state = 2; // TCP connect completed, in which case send the MQTT connect packet
		MQTTSPacket_send_connect(client);
		time(&(client->lastContact));
	}
	FUNC_EXIT;
}



int MQTTSProtocol_handleConnacks(void* pack, int sock, char* clientAddr)
{
	MQTTS_Connack* connack = (MQTTS_Connack*)pack;
	int rc = 0;
	Clients* client = NULL;
	BridgeConnections* bc = Bridge_getBridgeConnection(sock);

	FUNC_ENTRY;
	if (bc == NULL)
	{
		rc = SOCKET_ERROR;
		goto exit;
	}

	if (bc->primary && bc->primary->socket == sock)
		client = bc->primary;
	else
		client = bc->backup;

	if (connack->returnCode != MQTTS_RC_ACCEPTED)
	{
		if (client)
		{
			if (client->noLocal == 1)
				client->noLocal = 0;
			else
				Log(LOG_WARNING, 132, NULL, connack->returnCode, client->clientID);
			MQTTProtocol_closeSession(client, 0);
		}
		else
			Socket_close(sock);
	}
	else if (client)
	{
		ListElement* addr = bc->cur_address;

		if (client == bc->primary && bc->round_robin == 0)
			addr = bc->addresses->first;
		Log(LOG_INFO, 133, NULL, bc->name, (char*)(addr->content));

		client->connect_state = 3;  //should have been 2 before
		bc->last_connect_result = connack->returnCode;
		(bc->no_successful_connections)++;
		client->connected = 1;
		client->good = 1;
		client->ping_outstanding = 0;
		time(&(client->lastContact));


		if (client->will)
		{
			Publish pub;
			Messages* m = NULL;
			MQTTProtocol_sys_publish(client->will->topic, "1");
			pub.payload = "1";
			pub.payloadlen = 1;
			pub.topic = client->will->topic;
			MQTTProtocol_startOrQueuePublish(client, &pub, 0, client->will->retained, &m);
		}


		if (client == bc->primary &&
			(bc->backup && (bc->backup->connected == 1 || bc->backup->connect_state != 0)))
		{
			Log(LOG_INFO, 134, NULL, (char*)(bc->cur_address->content));
			MQTTProtocol_closeSession(bc->backup, 0);
		}

		//if (bc->addresses->count > 1 || bc->no_successful_connections == 1)
		Bridge_subscribe(bc, client);
	}

	//Clients* client = Protocol_getclientbyaddr(clientAddr);

	MQTTSPacket_free_packet(pack);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}



int MQTTSProtocol_handleWillTopicReqs(void* pack, int sock, char* clientAddr)
{
	Clients* client = Bridge_getClient(sock);

	FUNC_ENTRY;
	if (client == NULL)
	{
		//TODO: what now?
	}
	else
		MQTTSPacket_send_willTopic(client);

	MQTTSPacket_free_packet(pack);
  	FUNC_EXIT;
  	return 0;
}

int MQTTSProtocol_handleWillMsgReqs(void* pack, int sock, char* clientAddr)
{
	Clients* client = Bridge_getClient(sock);

	FUNC_ENTRY;
	if (client == NULL)
	{
		//TODO: what now?
	}
	else
		MQTTSPacket_send_willMsg(client);

	MQTTSPacket_free_packet(pack);
  	FUNC_EXIT;
  	return 0;
}

int MQTTSProtocol_handleSubacks(void* pack, int sock, char* clientAddr)
{
	int rc = 0;
	Clients* client = Protocol_getclientbyaddr(clientAddr);
	MQTTS_SubAck* suback = (MQTTS_SubAck*)pack;

	FUNC_ENTRY;
	if (client->pendingSubscription == NULL ||
			suback->msgId != client->pendingSubscription->msgId)
	{
		/* TODO: unexpected suback*/
	}
	else if (suback->returnCode != MQTTS_RC_ACCEPTED)
	{
		/* TODO: rejected ack... what do we do now? */
	}
	else
	{
		if (suback->topicId > 0)
		{
			Registration* reg = malloc(sizeof(Registration));
			reg->topicName = client->pendingSubscription->topicName;
			reg->id = suback->topicId;
			ListAppend(client->registrations, reg, sizeof(reg) + strlen(reg->topicName)+1);
		}
		else
			free(client->pendingSubscription->topicName);
		free(client->pendingSubscription);
		client->pendingSubscription = NULL;
		/* TODO: could proactively call Bridge_subscribe() */
	}

	time( &(client->lastContact) );
	MQTTSPacket_free_packet(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_startSubscription(Clients* client, char* topic, int qos)
{
	PendingSubscription* pendingSub = malloc(sizeof(PendingSubscription));
	int msgId = MQTTProtocol_assignMsgId(client);
	int rc = 0;

	FUNC_ENTRY;
	pendingSub->msgId = msgId;
	pendingSub->topicName = topic;
	pendingSub->qos = qos;
	time(&(pendingSub->sent));
	client->pendingSubscription = pendingSub;
	rc = MQTTSPacket_send_subscribe(client, topic, qos, msgId);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSProtocol_startClientRegistration(Clients* client, char* topic)
{
	PendingRegistration* pendingReg = malloc(sizeof(PendingRegistration));
	Registration* reg;
	int msgId = MQTTProtocol_assignMsgId(client);
	char* regTopicName = malloc(strlen(topic)+1);
	int rc = 0;

	FUNC_ENTRY;
	strcpy(regTopicName,topic);

	reg = malloc(sizeof(Registration));
	reg->topicName = regTopicName;
	reg->id = 0;

	pendingReg->msgId = msgId;
	pendingReg->registration = reg;
	time(&(pendingReg->sent));
	client->pendingRegistration = pendingReg;
	rc = MQTTSPacket_send_register(client, reg->id, regTopicName, msgId);
	FUNC_EXIT_RC(rc);
	return rc;
}

#endif
