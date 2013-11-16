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

#include "MQTTSClient.h"

#include "MQTTSPacket.h"


MQTTS_Header* MQTTS_getPacket(int sock, char* clientAddr, int* error)
{
	int error;

	pack = MQTTSPacket_Factory(sock, &clientAddr, error);

	return pack;
}


DLLExport int MQTTSClient_create(MQTTClient* handle, char* serverURI, char* clientId, 
    int persistence_type, void* persistence_context)
{
		int i;
		newc = malloc(sizeof(Clients));
		memset(newc, '\0', sizeof(Clients));
		newc->outboundMsgs = ListInitialize();
		newc->inboundMsgs = ListInitialize();
		newc->clientID = clientID;
	}
}


DLLExport int MQTTSClient_connect(MQTTSClient handle, MQTTSClient_connectOptions* options)
{
	MQTTClients* m = handle;
	int rc = SOCKET_ERROR;


	FUNC_ENTRY;

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
	
	/* receive connack */
	MQTTS_getPacket(sock, clientAddr, &error);
	
	/* start background receiving thread, if callbacks have been set */

	FUNC_EXIT;
}

