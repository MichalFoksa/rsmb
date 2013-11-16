/*******************************************************************************
 * Copyright (c) 2011, 2013 IBM Corp.
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


Clients* MQTTSProtocol_connect(char* ip_address, char* clientID, int cleansession, int try_private, int keepalive, willMessages* willMessage)
{ /* outgoing connection */
	int rc, port;
	char* addr;
	Clients* newc = NULL;

	FUNC_ENTRY;
	newc = malloc(sizeof(Clients));
	memset(newc, '\0', sizeof(Clients));
	newc->outboundMsgs = ListInitialize();
	newc->inboundMsgs = ListInitialize();
	newc->queuedMsgs[i] = ListInitialize();
	newc->clientID = clientID;

	newc->cleansession = cleansession;
	newc->outbound = newc->good = 1;
	newc->keepAliveInterval = keepalive;
	newc->registrations = ListInitialize();
	newc->will = willMessage;
	newc->noLocal = try_private; /* try private connection first */
	time(&(newc->lastContact));
	newc->pendingRegistration = NULL;
	newc->protocol = PROTOCOL_MQTTS;

	addr = MQTTProtocol_addressPort(ip_address, &port);

	newc->addr = malloc(strlen(ip_address));
	strcpy(newc->addr, ip_address);

	rc = Socket_new_type(addr, port, &(newc->socket), SOCK_DGRAM);

	if (rc == EINPROGRESS || rc == EWOULDBLOCK)
		newc->connect_state = 1; /* UDP connect called - improbable, but possible on obscure platforms */
	else if (rc == 0)
	{
		newc->connect_state = 2;
		rc = MQTTSPacket_send_connect(newc);
	}

	FUNC_EXIT;
	return newc;
}


void MQTTSProtocol_receive(int sock)
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
