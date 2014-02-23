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
 * Functions dealing with the MQTT protocol exchanges.
 * Some other related functions are in the MQTTProtocolClient module
 */

#if !defined(NO_BRIDGE)

#include <stdlib.h>

#include "MQTTProtocolOut.h"
#include "StackTrace.h"
#include "Heap.h"

extern MQTTProtocol state;	/**< MQTTProtocol state - shared with MQTTProtocol module */
extern BrokerStates* bstate;	/**< Broker state - shared with MQTTProtocol module */


/**
 * Separates an address:port into two separate values
 * @param ip_address the input string
 * @param port the returned port integer
 * @return the address string
 */
char* MQTTProtocol_addressPort(char* ip_address, int* port)
{
	static char buf[INET6_ADDRSTRLEN+1];
	char* pos = strrchr(ip_address, ':'); /* reverse find to allow for ':' in IPv6 addresses */
	int len;

	FUNC_ENTRY;
	if (ip_address[0] == '[')
	{  /* ip v6 */
		if (pos < strrchr(ip_address, ']'))
			pos = NULL;  /* means it was an IPv6 separator, not for host:port */
	}

	if (pos)
	{
		int len = pos - ip_address;
		*port = atoi(pos+1);
		strncpy(buf, ip_address, len);
		buf[len] = '\0';
		pos = buf;
	}
	else
	{
		*port = DEFAULT_PORT;
	  pos = ip_address;
	}

	len = strlen(buf);
	if (buf[len - 1] == ']')
		buf[len - 1] = '\0';
	FUNC_EXIT;
	return pos;
}


/**
 * Reconnect a bridge connection
 * @param ip_address the TCP address:port to connect to
 * @param client the client information to use
 */
void MQTTProtocol_reconnect(char* ip_address, Clients* client)
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

	rc = Socket_new(address, port, &(client->socket));
	if (rc == EINPROGRESS || rc == EWOULDBLOCK)
		client->connect_state = 1; /* TCP connect called */
	else if (rc == 0)
	{
		client->connect_state = 2; /* TCP connect completed, in which case send the MQTT connect packet */
		rc = MQTTPacket_send_connect(client);
	}
	/* lastContact is used for reconnection attempts, as well as keepalive, so we need to set it if we
	failed to connect, otherwise it is set in MQTTPacket_send_connect */
	if (rc != TCPSOCKET_COMPLETE)
		time(&(client->lastContact));
	FUNC_EXIT;
}


/**
 * MQTT outgoing connect processing for a client
 * @param ip_address the TCP address:port to connect to
 * @param clientID the MQTT client id to use
 * @param cleansession MQTT cleansession flag
 * @param try_private boolean always 0 for client
 * @param keepalive MQTT keepalive timeout in seconds
 * @param willMessage pointer to the will message to be used, if any
 * @param username pointer, or NULL for unauthenticated connections
 * @param password pointer, or NULL for unauthenticated connections
 * @return the new client structure
 */
Clients* MQTTProtocol_connect(char* ip_address, char* clientID, int cleansession, int try_private, int keepalive, willMessages* willMessage, char* username, char* password)
{ /* outgoing connection */
	int rc, port;
	char* addr;
	Clients* newc = NULL;
	ListElement* elem = NULL;

	FUNC_ENTRY;
	elem = ListFindItem(bstate->clients, clientID, clientIDCompare);
	if (elem != NULL)
	{
		newc = (Clients*)(elem->content); /* must be a dummy client */
		free(clientID);
		newc->connected = newc->ping_outstanding = newc->connect_state = newc->msgID = newc->discardedMsgs = 0;
	}
	else
	{
		int i;
		
		newc = malloc(sizeof(Clients));
		memset(newc, '\0', sizeof(Clients));
		newc->outboundMsgs = ListInitialize();
		newc->inboundMsgs = ListInitialize();
		for (i = 0; i < PRIORITY_MAX; ++i)
			newc->queuedMsgs[i] = ListInitialize();
		newc->clientID = clientID;
		ListAppend(bstate->clients, newc, sizeof(Clients) + strlen(newc->clientID)+1 + 3*sizeof(List));
	}
	newc->outbound = newc->good = 1;
	newc->keepAliveInterval = keepalive;
	newc->cleansession = cleansession;
	newc->will = willMessage;
	newc->noLocal = try_private; /* try private connection first */
	newc->username = username;
	newc->password = password;
#if defined(MQTTS)
	newc->protocol = PROTOCOL_MQTT;
#endif
	addr = MQTTProtocol_addressPort(ip_address, &port);
	rc = Socket_new(addr, port, &(newc->socket));
	if (rc == EINPROGRESS || rc == EWOULDBLOCK)
		newc->connect_state = 1; /* TCP connect called */
	else if (rc == 0)
	{
		newc->connect_state = 2; /* TCP connect completed, in which case send the MQTT connect packet */
		rc = MQTTPacket_send_connect(newc);
	}
	/* lastContact is used for reconnection attempts, as well as keepalive, so we need to set it if we
	failed to connect, otherwise it is set in MQTTPacket_send_connect */
	if (rc != TCPSOCKET_COMPLETE)
		time(&(newc->lastContact));
	FUNC_EXIT;
	return newc;
}


/**
 * Process an incoming pingresp packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePingresps(void* pack, int sock)
{
	Clients* client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 21, NULL, sock, client->clientID);
	if (client->outbound == 0)
		Log(LOG_WARNING, 71, NULL, client->clientID);
	else
		client->ping_outstanding = 0;
	FUNC_EXIT;
	return TCPSOCKET_COMPLETE;
}


/**
 * MQTT outgoing subscribe processing for a client
 * @param client the client structure
 * @param topics list of topics
 * @param qoss corresponding list of QoSs
 * @return completion code
 */
int MQTTProtocol_subscribe(Clients* client, List* topics, List* qoss)
{
	int rc = 0;

	FUNC_ENTRY;
	/* we should stack this up for retry processing too? */
	rc = MQTTPacket_send_subscribe(topics, qoss, MQTTProtocol_assignMsgId(client), 0, client->socket, client->clientID);
	if (rc == TCPSOCKET_COMPLETE)
		time(&(client->lastContact));
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming suback packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleSubacks(void* pack, int sock)
{
	Suback* suback = (Suback*)pack;
	Clients* client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 23, NULL, sock, client->clientID, suback->msgId);
	MQTTPacket_freeSuback(suback);
	FUNC_EXIT;
	return TCPSOCKET_COMPLETE;
}


/**
 * MQTT outgoing unsubscribe processing for a client
 * @param client the client structure
 * @param topics list of topics
 * @return completion code
 */
int MQTTProtocol_unsubscribe(Clients* client, List* topics)
{
	int rc = 0;

	FUNC_ENTRY;
	/* we should stack this up for retry processing too? */
	rc = MQTTPacket_send_unsubscribe(topics, MQTTProtocol_assignMsgId(client), 0, client->socket, client->clientID);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming unsuback packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handleUnsubacks(void* pack, int sock)
{
	Unsuback* unsuback = (Unsuback*)pack;
	Clients* client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);

	FUNC_ENTRY;
	Log(LOG_PROTOCOL, 24, NULL, sock, client->clientID, unsuback->msgId);
	free(unsuback);
	FUNC_EXIT;
	return TCPSOCKET_COMPLETE;
}

#endif
