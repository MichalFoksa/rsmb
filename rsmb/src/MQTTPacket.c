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
 * Functions to deal with reading and writing of MQTT packets from and to sockets.
 * Some other related functions are in the MQTTPacketOut module
 */

#include "MQTTPacket.h"
#include "Log.h"
#include "Clients.h"
#include "Messages.h"
#include "StackTrace.h"

#include <stdlib.h>
#include <string.h>

#include "Heap.h"

/**
 * List of the predefined MQTT v3 packet names.
 */
static char* packet_names[] =
{
	"RESERVED", "CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL",
	"PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK",
	"PINGREQ", "PINGRESP", "DISCONNECT"
};


/**
 * Converts an MQTT packet code into its name
 * @param ptype packet code
 * @return the corresponding string, or "UNKNOWN"
 */
char* MQTTPacket_name(int ptype)
{
	return (ptype >= 0 && ptype <= DISCONNECT) ? packet_names[ptype] : "UNKNOWN";
}

/**
 * function signature for the new_packets table
 */
typedef void* (*pf)(unsigned char, char*, int);

/**
 * Array of functions to build packets, indexed according to packet code
 */
#if defined(NO_BRIDGE)
static pf new_packets[] =
{
	NULL,					/**< reserved */
	MQTTPacket_connect,		/**< CONNECT */
	NULL,  					/**< CONNACK */
	MQTTPacket_publish,		/**< PUBLISH */
	MQTTPacket_ack, 		/**< PUBACK */
	MQTTPacket_ack, 		/**< PUBREC */
	MQTTPacket_ack, 		/**< PUBREL */
	MQTTPacket_ack, 		/**< PUBCOMP */
	MQTTPacket_subscribe,	/**< SUBSCRIBE */
	NULL, 					/**< SUBACK */
	MQTTPacket_unsubscribe,	/**< UNSUBSCRIBE */
	MQTTPacket_ack, 		/**< UNSUBACK */
	MQTTPacket_header_only, /**< PINGREQ */
	MQTTPacket_header_only, /**< PINGRESP */
	MQTTPacket_header_only  /**< DISCONNECT */
};
#else
static pf new_packets[] =
{
	NULL,					/**< reserved */
	MQTTPacket_connect,		/**< CONNECT */
	MQTTPacket_connack,		/**< CONNACK */
	MQTTPacket_publish,		/**< PUBLISH */
	MQTTPacket_ack, 		/**< PUBACK */
	MQTTPacket_ack, 		/**< PUBREC */
	MQTTPacket_ack, 		/**< PUBREL */
	MQTTPacket_ack, 		/**< PUBCOMP */
	MQTTPacket_subscribe,	/**< SUBSCRIBE */
	MQTTPacket_suback,		/**< SUBACK */
	MQTTPacket_unsubscribe,	/**< UNSUBSCRIBE */
	MQTTPacket_ack, 		/**< UNSUBACK */
	MQTTPacket_header_only, /**< PINGREQ */
	MQTTPacket_header_only, /**< PINGRESP */
	MQTTPacket_header_only  /**< DISCONNECT */
};
#endif


/**
 * Reads one MQTT packet from a socket.
 * @param socket a socket from which to read an MQTT packet
 * @param error pointer to the error code which is completed if no packet is returned
 * @return the packet structure or NULL if there was an error
 */
void* MQTTPacket_Factory(int socket, int* error)
{
	char* data = NULL;
	static Header header;
	int remaining_length, ptype;
	void* pack = NULL;
	int actual_len = 0;
	NewSockets* new = NULL;


	FUNC_ENTRY;
	*error = SOCKET_ERROR;  /* indicate whether an error occurred, or not */

	/* read the packet data from the socket */
	if ((*error = Socket_getch(socket, &(header.byte))) != TCPSOCKET_COMPLETE)   /* first byte is the header byte */
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */
		
	if ((new = Socket_getNew(socket)) && new->outbound == 0 && header.bits.type != CONNECT)
	{
		Log(LOG_WARNING, 23, NULL, socket, Socket_getpeer(socket), MQTTPacket_name(header.bits.type));
		*error = SOCKET_ERROR;
		goto exit;
	}

	/* now read the remaining length, so we know how much more to read */
	if ((*error = MQTTPacket_decode(socket, &remaining_length)) != TCPSOCKET_COMPLETE)
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	/* now read the rest, the variable header and payload */
	if ((data = Socket_getdata(socket, remaining_length, &actual_len)) == NULL)
	{
		*error = SOCKET_ERROR;
		goto exit; /* socket error */
	}

	if (actual_len != remaining_length)
		*error = TCPSOCKET_INTERRUPTED;
	else
	{
		ptype = header.bits.type;
		if (ptype < CONNECT || ptype > DISCONNECT || new_packets[ptype] == NULL)
			Log(TRACE_MAX, 17, NULL, ptype);
		else
		{
			if ((pack = (*new_packets[ptype])(header.byte, data, remaining_length)) == NULL)
				*error = BAD_MQTT_PACKET;
		}
	}
exit:
	FUNC_EXIT_RC(*error);
	return pack;
}


/**
 * Sends an MQTT packet in one system call write
 * @param socket the socket to which to write the data
 * @param header the one-byte MQTT header
 * @param buffer the rest of the buffer to write (not including remaining length)
 * @param buflen the length of the data in buffer to be written
 * @return the completion code (TCPSOCKET_COMPLETE etc)
 */
int MQTTPacket_send(int socket, Header header, char* buffer, int buflen)
{
	int rc, buf0len;
	char *buf;

	FUNC_ENTRY;
	buf = malloc(10);
	buf[0] = header.byte;
	buf0len = 1 + MQTTPacket_encode(&buf[1], buflen);
	rc = Socket_putdatas(socket, buf, buf0len, 1, &buffer, &buflen);
	if (rc != TCPSOCKET_INTERRUPTED)
	  free(buf);

	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Sends an MQTT packet from multiple buffers in one system call write
 * @param socket the socket to which to write the data
 * @param header the one-byte MQTT header
 * @param count the number of buffers
 * @param buffers the rest of the buffers to write (not including remaining length)
 * @param buflens the lengths of the data in the array of buffers to be written
 * @return the completion code (TCPSOCKET_COMPLETE etc)
 */
int MQTTPacket_sends(int socket, Header header, int count, char** buffers, int* buflens)
{
	int i, rc, buf0len, total = 0;
	char *buf;

	FUNC_ENTRY;
	buf = malloc(10);
	buf[0] = header.byte;
	for (i = 0; i < count; i++)
		total += buflens[i];
	buf0len = 1 + MQTTPacket_encode(&buf[1], total);
	rc = Socket_putdatas(socket, buf, buf0len, count, buffers, buflens);
	if (rc != TCPSOCKET_INTERRUPTED)
	  free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Encodes the message length according to the MQTT algorithm
 * @param buf the buffer into which the encoded data is written
 * @param length the length to be encoded
 * @return the number of bytes written to buffer
 */
int MQTTPacket_encode(char* buf, int length)
{
	int rc = 0;
	FUNC_ENTRY;

	do
	{
		char d = length % 128;
		length /= 128;
		/* if there are more digits to encode, set the top bit of this digit */
		if (length > 0)
			d |= 0x80;
		buf[rc++] = d;
	} while (length > 0);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Decodes the message length according to the MQTT algorithm
 * @param socket the socket from which to read the bytes
 * @param value the decoded length returned
 * @return the number of bytes read from the socket
 */
int MQTTPacket_decode(int socket, int* value)
{
	int rc = SOCKET_ERROR;
	char c;
	int multiplier = 1;
	int len = 0;
#define MAX_NO_OF_REMAINING_LENGTH_BYTES 4

	FUNC_ENTRY;
	*value = 0;
	do
	{
		if (++len > MAX_NO_OF_REMAINING_LENGTH_BYTES)
		{
			rc = SOCKET_ERROR;	/* bad data */
			goto exit;
		}
		if ((rc = Socket_getch(socket, &c)) != TCPSOCKET_COMPLETE)
			goto exit;
		*value += (c & 127) * multiplier;
		multiplier *= 128;
	} while ((c & 128) != 0);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Calculates an integer from two bytes read from the input buffer
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the integer value calculated
 */
int readInt(char** pptr)
{
	char* ptr = *pptr;
	int len = 256*((unsigned char)(*ptr)) + (unsigned char)(*(ptr+1));

	*pptr += 2;
	return len;
}


/**
 * Reads a "UTF" string from the input buffer.  UTF as in the MQTT v3 spec which really means
 * a length delimited string.  So it reads the two byte length then the data according to
 * that length.  The end of the buffer is provided too, so we can prevent buffer overruns caused
 * by an incorrect length.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @param enddata pointer to the end of the buffer not to be read beyond
 * @param len returns the calculcated value of the length bytes read
 * @return an allocated C string holding the characters read, or NULL if the length read would
 * have caused an overrun.
 */
char* readUTFlen(char** pptr, char* enddata, int* len)
{
	char* string = NULL;

	FUNC_ENTRY;
	if (enddata - (*pptr) > 1) /* enough length to read the integer? */
	{
		*len = readInt(pptr);
		if (&(*pptr)[*len] <= enddata)
		{
			string = malloc(*len+1);
			memcpy(string, *pptr, *len);
			string[*len] = '\0';
			*pptr += *len;
		}
	}
	FUNC_EXIT;
	return string;
}


/**
 * Reads a "UTF" string from the input buffer.  UTF as in the MQTT v3 spec which really means
 * a length delimited string.  So it reads the two byte length then the data according to
 * that length.  The end of the buffer is provided too, so we can prevent buffer overruns caused
 * by an incorrect length.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @param enddata pointer to the end of the buffer not to be read beyond
 * @return an allocated C string holding the characters read, or NULL if the length read would
 * have caused an overrun.
 */
char* readUTF(char** pptr, char* enddata)
{
	int len;
	return readUTFlen(pptr, enddata, &len);
}


/**
 * Reads one character from the input buffer.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the character read
 */
char readChar(char** pptr)
{
	char c = **pptr;
	(*pptr)++;
	return c;
}


/**
 * Writes one character to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param c the character to write
 */
void writeChar(char** pptr, char c)
{
	**pptr = c;
	(*pptr)++;
}


/**
 * Writes an integer as 2 bytes to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param anInt the integer to write
 */
void writeInt(char** pptr, int anInt)
{
	**pptr = (char)(anInt / 256);
	(*pptr)++;
	**pptr = (char)(anInt % 256);
	(*pptr)++;
}


/**
 * Writes a "UTF" string to an output buffer.  Converts C string to length-delimited.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param string the C string to write
 */
void writeUTF(char** pptr, char* string)
{
	int len = strlen(string);
	writeInt(pptr, len);
	memcpy(*pptr, string, len);
	*pptr += len;
}


/**
 * Checks whether the incoming connect packet's protocol name and version are valid
 * @param pack pointer to the incoming connect packet
 * @return boolean - is the version acceptable?
 */
int MQTTPacket_checkVersion(Connect* pack)
{
	return (strcmp(pack->Protocol, "MQIsdp") == 0 &&
		(pack->version == 3 || pack->version == PRIVATE_PROTOCOL_VERSION)) ||
				(strcmp(pack->Protocol, "MQIpdp") == 0 && pack->version == 2) ||
		(strcmp(pack->Protocol, "MQTT") == 0 && pack->version == 4);
}


/**
 * Function used in the new packets table to create connect packets.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_connect(unsigned char aHeader, char* data, int datalen)
{
	Connect* pack = malloc(sizeof(Connect));
	char* curdata = data;
	char* enddata = &data[datalen];
	int error = 1;

	FUNC_ENTRY;
	memset(pack, '\0', sizeof(Connect));
	pack->header.byte = aHeader;
	if ((pack->Protocol = readUTF(&curdata, enddata)) == NULL || /* should be "MQIsdp" */
		enddata - curdata < 0) /* can we read protocol version char? */
		goto exit;
	pack->version = (int)readChar(&curdata); /* Protocol version */
	/* If we don't recognize the protocol version, we don't parse the connect packet on the
	 * basis that we don't know what the format will be.
	 */
	if (MQTTPacket_checkVersion(pack))
	{
		pack->flags.all = readChar(&curdata);
		pack->keepAliveTimer = readInt(&curdata);
		if ((pack->clientID = readUTF(&curdata, enddata)) == NULL)
			goto exit;
		if (pack->flags.bits.will)
		{
			if ((pack->willTopic = readUTF(&curdata, enddata)) == NULL ||
				  (pack->willMsg = readUTF(&curdata, enddata)) == NULL)
				goto exit;
			Log(TRACE_MAX, 18, NULL, pack->willTopic, pack->willMsg, pack->flags.bits.willRetain);
		}
		if (pack->flags.bits.username)
		{
			if (enddata - curdata < 3 || (pack->username = readUTF(&curdata,enddata)) == NULL)
				goto exit; /* username flag set, but no username supplied - invalid */
			if (pack->flags.bits.password &&
					(enddata - curdata < 3 || (pack->password = readUTF(&curdata,enddata)) == NULL))
				goto exit; /* password flag set, but no password supplied - invalid */
		}
		else if (pack->flags.bits.password)
			goto exit; /* password flag set without username - invalid */
	}
	error = 0; /* If we've reached here, then we don't free the packet in this function */
exit:
	if (error)
	{
		MQTTPacket_freeConnect(pack);
		pack = NULL;
	}
	FUNC_EXIT;
	return (void*)pack;
}


/**
 * Free allocated storage for a connect packet.
 * @param pack pointer to the connect packet structure
 */
void MQTTPacket_freeConnect(Connect* pack)
{
	FUNC_ENTRY;
	if (pack->Protocol != NULL)
		free(pack->Protocol);
	if (pack->clientID != NULL)
		free(pack->clientID);
	if (pack->flags.bits.will)
	{
		if (pack->willTopic != NULL)
			free(pack->willTopic);
		if (pack->willMsg != NULL)
			free(pack->willMsg);
	}
	if (pack->username != NULL)
		free(pack->username);
	if (pack->password != NULL)
		free(pack->password);
	free(pack);
	FUNC_EXIT;
}


/**
 * Function used in the new packets table to create packets which have only a header.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_header_only(unsigned char aHeader, char* data, int datalen)
{
	static unsigned char header = 0;
	header = aHeader;
	return &header;
}


/**
 * Send an MQTT pingresp packet down a socket.
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pingresp(int socket, char* clientID)
{
	Header header;
	int rc = 0;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = PINGRESP;
	rc =  MQTTPacket_send(socket, header, NULL, 0);
	Log(LOG_PROTOCOL, 4, NULL, socket, clientID, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT disconnect packet down a socket.
 * @param socket the open socket to send the data to
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_disconnect(int socket, char* clientID)
{
	Header header;
	int rc = 0;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = DISCONNECT;
	rc = MQTTPacket_send(socket, header, NULL, 0);
	Log(LOG_PROTOCOL, 28, NULL, socket, clientID, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create subscribe structures from packet data.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_subscribe(unsigned char aHeader, char* data, int datalen)
{
	Subscribe* pack = malloc(sizeof(Subscribe));
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	pack->header.byte = aHeader;
	pack->msgId = readInt(&curdata);
	pack->topics = ListInitialize();
	pack->qoss = ListInitialize();
	pack->noTopics = 0;
	while (curdata - data < datalen)
	{
		int* newint;
		int len;
		char* str = readUTFlen(&curdata, enddata, &len);
		if (str == NULL || enddata - curdata < 1)
		{
			MQTTPacket_freeSubscribe(pack, 1);
			pack = NULL;
			goto exit;
		}
		ListAppend(pack->topics, str, len);
		newint = malloc(sizeof(int));
		*newint = (int)readChar(&curdata);
		if (*newint == 3)
		{
			free(newint);
			MQTTPacket_freeSubscribe(pack, 1);
			pack = NULL;
			goto exit;
		}
		ListAppend(pack->qoss, newint, sizeof(int));
		(pack->noTopics)++;
	}
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Free allocated storage for a subscribe packet.
 * @param pack pointer to the subscribe packet structure
 * @param all boolean flag to indicate whether the topic names should be freed
 */
void MQTTPacket_freeSubscribe(Subscribe* pack, int all)
{
	FUNC_ENTRY;
	if (pack->topics != NULL && all)
		ListFree(pack->topics);
	else
		ListFreeNoContent(pack->topics);
	if (pack->qoss != NULL)
		ListFree(pack->qoss);
	free(pack);
	FUNC_EXIT;
}


/**
 * Send an MQTT subscribe acknowledgement packet down a socket.
 * @param msgid the MQTT message id to use
 * @param noOfTopics no of items in the qoss list
 * @param qoss list of corresponding QoSs
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_suback(int msgid, int noOfTopics, int* qoss, int socket, char* clientID)
{
	Header header;
	char *data, *ptr;
	int i, datalen, rc;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = SUBACK;
	datalen = 2 + noOfTopics;
	ptr = data = malloc(datalen);
	writeInt(&ptr, msgid);
	for (i = 0; i < noOfTopics; i++)
		writeChar(&ptr, (char)(qoss[i]));
	rc = MQTTPacket_send(socket, header, data, datalen);
	Log(LOG_PROTOCOL, 7, NULL, socket, clientID, msgid, rc);
	free(data);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free allocated storage for a suback packet.
 * @param pack pointer to the suback packet structure
 */
void MQTTPacket_freeSuback(Suback* pack)
{
	FUNC_ENTRY;
	if (pack->qoss != NULL)
		ListFree(pack->qoss);
	free(pack);
	FUNC_EXIT;
}


/**
 * Function used in the new packets table to create unsubscribe structures from packet data.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_unsubscribe(unsigned char aHeader, char* data, int datalen)
{
	Unsubscribe* pack = malloc(sizeof(Unsubscribe));
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	pack->header.byte = aHeader;
	pack->msgId = readInt(&curdata);
	pack->noTopics = 0;
	pack->topics = NULL;
	pack->topics = ListInitialize();
	while (curdata - data < datalen)
	{
		int len;
		char* str = readUTFlen(&curdata, enddata, &len);
		if (str == NULL)
		{
			MQTTPacket_freeUnsubscribe(pack);
			pack = NULL;
			goto exit;
		}
		ListAppend(pack->topics, str, len);
		(pack->noTopics)++;
	}
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Free allocated storage for an unsubscribe packet.
 * @param pack pointer to the unsubscribe packet structure
 */
void MQTTPacket_freeUnsubscribe(Unsubscribe* pack)
{
	FUNC_ENTRY;
	if (pack->topics != NULL)
		ListFree(pack->topics);
	free(pack);
	FUNC_EXIT;
}


/**
 * Function used in the new packets table to create publish packets.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_publish(unsigned char aHeader, char* data, int datalen)
{
	Publish* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];
	Header head = {aHeader};

	FUNC_ENTRY;
	if (head.bits.qos == 3)
		goto exit;
	pack = malloc(sizeof(Publish));
	pack->header.byte = aHeader;
	//if ((pack->topic = readUTFlen(&curdata, enddata, &pack->topiclen)) == NULL) /* Topic name on which to publish */
	if ((pack->topic = readUTF(&curdata, enddata)) == NULL) /* Topic name on which to publish */
	{
		free(pack);
		pack = NULL;
		goto exit;
	}
	if (pack->header.bits.qos > 0)  /* Msgid only exists for QoS 1 or 2 */
		pack->msgId = readInt(&curdata);
	else
		pack->msgId = 0;
	pack->payload = curdata;
	pack->payloadlen = datalen-(curdata-data);
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Free allocated storage for a publish packet.
 * @param pack pointer to the publish packet structure
 */
void MQTTPacket_freePublish(Publish* pack)
{
	FUNC_ENTRY;
	if (pack->topic != NULL)
		free(pack->topic);
	free(pack);
	FUNC_EXIT;
}


/**
 * Send an MQTT acknowledgement packet down a socket.
 * @param type the MQTT packet type e.g. SUBACK
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param socket the open socket to send the data to
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_ack(int type, int msgid, int dup, int socket)
{
	Header header;
	int rc;
	char *buf = malloc(2);
	char *ptr = buf;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = type;
	header.bits.dup = dup;
 	if (type == PUBREL)
    	header.bits.qos = 1;
	writeInt(&ptr, msgid);
	if ((rc = MQTTPacket_send(socket, header, buf, 2)) != TCPSOCKET_INTERRUPTED)
		free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT connack packet down a socket.
 * @param aRc the connect return code
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_connack(int aRc, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(CONNACK, aRc, 0, socket);
	Log(LOG_PROTOCOL, 2, NULL, socket, clientID, aRc, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT unsuback packet down a socket.
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_unsuback(int msgid, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(UNSUBACK, msgid, 0, socket);
	Log(LOG_PROTOCOL, 9, NULL, socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT puback packet down a socket.
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_puback(int msgid, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc =  MQTTPacket_send_ack(PUBACK, msgid, 0, socket);
	Log(LOG_PROTOCOL, 12, NULL, socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT pubrec packet down a socket.
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubrec(int msgid, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc =  MQTTPacket_send_ack(PUBREC, msgid, 0, socket);
	Log(LOG_PROTOCOL, 13, NULL, socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT pubrel packet down a socket.
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubrel(int msgid, int dup, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(PUBREL, msgid, dup, socket);
	Log(LOG_PROTOCOL, 16, NULL, socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT pubcomp packet down a socket.
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubcomp(int msgid, int socket, char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(PUBCOMP, msgid, 0, socket);
	Log(LOG_PROTOCOL, 18, NULL, socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create acknowledgement packets.
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_ack(unsigned char aHeader, char* data, int datalen)
{
	Ack* pack = malloc(sizeof(Ack));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header.byte = aHeader;
	pack->msgId = readInt(&curdata);
	FUNC_EXIT;
	return pack;
}


/**
 * Send an MQTT PUBLISH packet down a socket.
 * @param pack a structure from which to get some values to use, e.g topic, payload
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param qos the value to use for the MQTT QoS setting
 * @param retained boolean - whether to set the MQTT retained flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_publish(Publish* pack, int dup, int qos, int retained, int socket, char* clientID)
{
	Header header;
	char *topiclen;
	int rc = -1;
	int topic_offset = 0;
#if !defined(SINGLE_LISTENER)
	Listener* listener = Socket_getParentListener(socket);
#endif

	FUNC_ENTRY;
#if !defined(SINGLE_LISTENER)
	if (listener && listener->mount_point)
	{
		if (strncmp(listener->mount_point, pack->topic, strlen(listener->mount_point)) != 0)
			Log(LOG_SEVERE, 13, "wrong listener topic %s", pack->topic);
		topic_offset = strlen(listener->mount_point);
	}
#endif

	topiclen = malloc(2);

	header.bits.type = PUBLISH;
	header.bits.dup = dup;
	header.bits.qos = qos;
	header.bits.retain = retained;
	if (qos > 0)
	{
		char *buf = malloc(2);
		char *ptr = buf;
		char* bufs[4] = {topiclen, (pack->topic) + topic_offset, buf, pack->payload};
		int lens[4] = {2, strlen(pack->topic) - topic_offset, 2, pack->payloadlen};
		writeInt(&ptr, pack->msgId);
		ptr = topiclen;
		writeInt(&ptr, lens[1]);
		rc = MQTTPacket_sends(socket, header, 4, bufs, lens);
		if (rc != TCPSOCKET_INTERRUPTED)
			free(buf);
	}
	else
	{
		char* ptr = topiclen;
		char* bufs[3] = {topiclen, (pack->topic) + topic_offset, pack->payload};
		int lens[3] = {2, strlen(pack->topic) - topic_offset, pack->payloadlen};
		writeInt(&ptr, lens[1]);
		rc = MQTTPacket_sends(socket, header, 3, bufs, lens);
	}
	if (rc != TCPSOCKET_INTERRUPTED)
		free(topiclen);
	if (strlen(pack->topic) < 15 || strncmp(pack->topic, "$SYS/broker/log", 15) != 0)
	{
		if (qos == 0)
			Log(LOG_PROTOCOL, 27, NULL, socket, clientID, retained, rc);
		else
			Log(LOG_PROTOCOL, 10, NULL, socket, clientID, pack->msgId, qos, retained, rc);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free allocated storage for a various packet types
 * @param pack pointer to a packet structure
 */
void MQTTPacket_free_packet(MQTTPacket* pack)
{
	FUNC_ENTRY;
	if (pack->header.bits.type == PUBLISH)
		MQTTPacket_freePublish((Publish*)pack);
	else if (pack->header.bits.type == SUBSCRIBE)
		MQTTPacket_freeSubscribe((Subscribe*)pack, 1);
	else if (pack->header.bits.type == SUBACK)
		MQTTPacket_freeSuback((Suback*)pack);
	else if (pack->header.bits.type == UNSUBSCRIBE)
		MQTTPacket_freeUnsubscribe((Unsubscribe*)pack);
	else if (pack->header.bits.type != DISCONNECT && pack->header.bits.type != PINGREQ && pack->header.bits.type != PINGRESP)
		free(pack);
	FUNC_EXIT;
}
