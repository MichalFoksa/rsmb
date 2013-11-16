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
 *    Nicholas O'Leary, Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

#if defined(MQTTMP)

#include "LinkedList.h"
#include "MQTTMPProtocol.h"
#include "MQTTProtocol.h"
#include "Log.h"
#include "Clients.h"
#include "Messages.h"
#include "Protocol.h"
#include "StackTrace.h"
#include "SocketBuffer.h"

#include "Heap.h"


/**
 * Broker state structure.
 */
BrokerStates* bstate;

/**
 * List to manage the available virtual socket numbers
 */
List* virtualSockets;
int nextVirtualSocket = -2;


/**
 * TODO: These two variables are used in a less than ideal fashion so that
 * MQTTProtocol module can find out what socket/channel is currently
 * being processed. Ideally, this information would be passed through
 * more directly.
 */
int realSocket;
int currentChannel = -1;

/**
 * Get the next available virtual socket number.
 * Virtual socket numbers are in the range -2 to -2147483647 (inclusive)
 */
int MQTTMPProtocol_getNextVirtualSocket()
{
	ListElement* elem;
	int* s = NULL;

	FUNC_ENTRY;
	do
	{
		if (nextVirtualSocket > -2147483647)
			nextVirtualSocket--;
		else
			nextVirtualSocket = -2;
	}
	while ((elem = ListFindItem(virtualSockets, &(nextVirtualSocket), intcompare)) != NULL);
	s = (int*)malloc(sizeof(int));
	*s = nextVirtualSocket;
	ListAppend(virtualSockets,s,sizeof(int));
	FUNC_EXIT;
	return nextVirtualSocket;
}

/**
 * Frees the virtual socket number for reuse
 */
void freeVirtualSocket(int socket)
{
	FUNC_ENTRY;
	ListRemoveItem(virtualSockets,&(socket),intcompare);
	FUNC_EXIT;
}

/**
 * Initializes the MQTTMPProtocol module
 */
void MQTTMPProtocol_initialize(BrokerStates* bs)
{
	FUNC_ENTRY;
	bstate = bs;
	virtualSockets = ListInitialize();
	FUNC_EXIT;
}

/**
 * Terminates the MPttMPProtocol module
 */
void MQTTMPProtocol_terminate()
{
	FUNC_ENTRY;
	Log(LOG_INFO, 209, NULL);
	ListFree(virtualSockets);
	FUNC_EXIT;
}

/**
 * A compare function for list searching, based on the 'actualSock'
 * field of Clients.
 * This finds any client connected to a specified MP listener.
 */
int clientActualSocketCompare(void* a, void* b)
{
	Clients* client = (Clients*)a;
	return client->actualSock == *((int*)b);
}

/**
 * A compare function for list searching, based on both the 'actualSock'
 * and 'channel' field of Clients.
 * This finds a specific client connected to a specified MP listener.
 * The second parameter should be an int[] containing {socket,channel}
 */
int clientActualSocketAndChannelCompare(void* a, void* b)
{
	Clients* client = (Clients*)a;
	return client->actualSock == ((int*)b)[0] && client->channel == ((int*)b)[1];
}

/**
 * Do any processing for the MP listener.
 *
 */
void MQTTMPProtocol_timeslice(int socket) {
	int error;
	char type;
	char ch_msb,ch_lsb;
	int channel;
	int clientLookup[2];
	int virtSocket;
	Clients* client = NULL;

	FUNC_ENTRY;
	realSocket = socket;

	/* read the packet data from the socket */
	if ((error = Socket_getch(socket, &(type))) != TCPSOCKET_COMPLETE)
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	/* read the channel msb from the socket */
	if ((error = Socket_getch(socket, &(ch_msb))) != TCPSOCKET_COMPLETE)
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	/* read the channel lsb from the socket */
	if ((error = Socket_getch(socket, &(ch_lsb))) != TCPSOCKET_COMPLETE)
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	channel = 256*((unsigned char)ch_msb)+((unsigned char)ch_lsb);

	currentChannel = channel;

	clientLookup[0] = socket;
	clientLookup[1] = channel;

    /* We do not log when a multiplexed connection is established; only when an MQTT client
       connection is made of the mp connection.
       The following block of code would add such a log statement in: */
	/* 
	if (ListFindItem(bstate->clients, &socket, clientActualSocketCompare) == NULL)
		Log(LOG_INFO,0,"New multiplexed connection on port %d from %s",Socket_getParentListener(socket)->port,Socket_getpeer(socket));
	*/

	if (ListFindItem(bstate->clients, &clientLookup, clientActualSocketAndChannelCompare) == NULL)
	{
		/* This is potentially a new client connecting - we don't recognise the channel */
		virtSocket = socket;
	}
	else
	{
		/* This is a known client based on the channel number. */
		client = (Clients*)(bstate->clients->current->content);
		virtSocket = client->socket;
	}

	if (type == MP_PACKET_MQTT)
		MQTTProtocol_timeslice(virtSocket, client);
	else if (type == MP_PACKET_CLOSE)
	{
		/* Nothing left to read from the socket */
		SocketBuffer_complete(socket);
		if (client != NULL)
		{
			if (bstate->connection_messages)
				Log(LOG_INFO, 200, NULL, client->clientID);
			MQTTMPProtocol_closeSession(client, 0);
			MQTTProtocol_closeSession(client, 1);
		}
	}
	else
	{
		Log(LOG_WARNING,201, NULL, type, socket, channel);
		MQTTMPProtocol_closeSocket(socket);
	}

exit:
	if (error == SOCKET_ERROR)
	{
		/* Any error at this point means a socket level issue that should trigger
		 * the complete shutdown of the TCP connection
		 * If there are no clients associated with the socket, this is likely to be
		 * an orderly disconnect/close, so don't log a warning.
		 */
		if (ListFindItem(bstate->clients, &socket, clientActualSocketCompare) != NULL)
			Log(LOG_WARNING, 202, NULL, Socket_getParentListener(socket)->port, socket);
		MQTTMPProtocol_closeSocket(socket);
	}

	/* No longer processing a client, so reset these variables */
	currentChannel = -1;
	realSocket = -1;
	FUNC_EXIT_RC(error);
}

/**
 * Closes the specified TCP socket. As this socket is a MP connection,
 * we need to tidy up each Client currently attached to it before
 * actually closing the socket.
 *
 * Currently, this is only called in response to a socket error,
 * so we don't attempt to send the channel close commands.
 */
void MQTTMPProtocol_closeSocket(int socket)
{
	ListElement* elem = NULL;

	FUNC_ENTRY;
	/* find the content */
	while (ListNextElement(bstate->clients, &elem) != NULL)
	{
		if (clientActualSocketCompare(elem->content, &socket))
		{
			Clients* client = (Clients*)(elem->content);
			if (client->good)
			{
				Log(LOG_WARNING,203,NULL,client->clientID,Socket_getParentListener(socket)->port);
				MQTTMPProtocol_closeSession(client,0);
				MQTTProtocol_closeSession(client,1);
			}
		}
	}
	Log(LOG_INFO,204,NULL,Socket_getParentListener(socket)->port);
	Socket_close(socket);
	FUNC_EXIT;
}

/**
 * Closes a session for a specific client.
 */
void MQTTMPProtocol_closeSession(Clients* client, int send_close)
{
	FUNC_ENTRY;
	if (send_close)
		MQTTMPProtocol_closeChannel(client->actualSock, client->channel);
	/* Log(LOG_INFO,0,"MQTTMPProtocol_closeSession id:%s vsock:%d asock:%d ch:%d",client->clientID,client->socket,client->actualSock,client->channel); */
	client->channel = 0;
	freeVirtualSocket(client->socket);
	FUNC_EXIT;
}


int MQTTMPProtocol_closeChannel(int socket, int channel)
{
	int rc = 0;
	char *buf = malloc(3);
	char *ptr = buf+1;
	FUNC_ENTRY;
	buf[0] = 1; /* CLOSE CHANNEL */
	writeInt(&ptr,channel);
	rc = Socket_putdatas(socket,buf,3,0,NULL,NULL,NULL);
	if (rc != TCPSOCKET_INTERRUPTED)
		free(buf);
	FUNC_EXIT_RC(rc);
	return rc;

}
/**
 * Potential temporary method #1
 * Returns the real socket the virtual socket is connected to.
 * This is only called by MQTTPacket_Factory so it can determine
 * the real socket to read from.
 */
int MQTTMPProtocol_get_real_socket(int virtSocket)
{
	int rc = virtSocket;
	if (virtSocket < 0)
		rc = realSocket;
	return rc;
}


/**
 * Potential temporary method #2
 * Returns the Clients instance for the specified socket.
 * This has evolved since the initial prototyping and now simply does
 * a lookup based on client->socket. As such, this is a candidate for
 * removing and doing the lookup inline.
 */
Clients* MQTTMPProtocol_getClientForVirtualSocket(int virtSocket)
{
	ListElement* elem = NULL;
	FUNC_ENTRY;
	elem = ListFindItem(bstate->clients,&virtSocket,clientSocketCompare);
	FUNC_EXIT;
	return (Clients*)(elem->content);
}

/**
 * Potential temporary method #3
 * Returns the current channel number being processed.
 * This is only called by MQTTProtocol_handleConnects so a
 * newly connected Client can have its channel number assigned.
 */
int MQTTMPProtocol_getCurrentChannel()
{
	return currentChannel;
}

/**
 * Send an MQTT connack packet to a multiplexed client
 * @param aRc the connect return code
 * @param socket the open socket to send the data to
 * @param channel the channel to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTMPProtocol_send_connack(int aRc, int socket, int channel, char* clientID)
{
	Header header;
	int rc = 0;
	char *msgIdbuf = malloc(2);
	char *headbuf = malloc(10);
	char *mpheader = malloc(3);
	char *bufs[2];
	int buflens[2];
	int buffree[2] = {1, 1};
	int buf0len;

	FUNC_ENTRY;

	header.byte = 0;
	header.bits.type = CONNACK;
	header.bits.dup = 0;

	msgIdbuf[0] = (char)(aRc / 256);
	msgIdbuf[1] = (char)(aRc % 256);

	headbuf[0] = header.byte;
	buf0len = 1 + MQTTPacket_encode(&headbuf[1], 2);

	bufs[0] = headbuf;
	bufs[1] = msgIdbuf;
	buflens[0] = buf0len;
	buflens[1] = 2;
	mpheader[0] = MP_PACKET_MQTT;
	mpheader[1] = (char)(channel / 256);
	mpheader[2] = (char)(channel % 256);

	rc = Socket_putdatas(realSocket, mpheader,3, 2, bufs, buflens, buffree);
	if (rc != TCPSOCKET_INTERRUPTED)
	{
		free(msgIdbuf);
		free(headbuf);
		free(mpheader);
	}
	Log(LOG_PROTOCOL, 2, NULL, socket, clientID, aRc, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}

#endif
