/*******************************************************************************
 * Copyright (c) 20012, 2013 IBM Corp.
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

#include "MQTTSPacket.h"
#include "Log.h"
#include "StackTrace.h"
#include "Heap.h"

#include "MQTTSPacketSerialize.h"


PacketBuffer MQTTSPacketSerialize_header(MQTTSHeader header)
{
	PacketBuffer buf;
	
	if (header.len > 256)
	{
		header.len += 2;
		buf.data = malloc(header.len);
		buf.ptr = buf.data;
		*(buf.ptr)++ = 0x01;
		writeInt(&buf.ptr, header.len);
	}
	else
	{
		buf.data = malloc(header.len);
		buf.ptr = buf.data;
		*(buf.ptr)++ = header.len;
	}
	buf.len = header.len;
	*(buf.ptr)++ = header.type;
	return buf;
}


PacketBuffer MQTTSPacketSerialize_ack(char type, int msgId)
{
	MQTTSHeader header;
	PacketBuffer buf;

	FUNC_ENTRY;
	if (msgId >= 0)
		header.len = 4;
	else
		header.len = 2;
	header.type = type;
	
	buf = MQTTSPacketSerialize_header(header);
	if (msgId >= 0)
		writeInt(&buf.ptr, msgId);

	FUNC_EXIT;
	return buf;
}


PacketBuffer MQTTSPacketSerialize_advertise(unsigned char gateway_id, short duration)
{
	MQTTSHeader header;
	PacketBuffer buf;
	
	FUNC_ENTRY;
	header.len = 5;
	header.type = MQTTS_ADVERTISE;
	
	buf = MQTTSPacketSerialize_header(header);

	writeChar(&buf.ptr, gateway_id);
	writeInt(&buf.ptr, duration);
	
	FUNC_EXIT;
	return buf;
}


PacketBuffer MQTTSPacketSerialize_connect(int cleansession, int will, char protocolID, short keepAlive, char* clientID)
{
	MQTTSHeader header;
	PacketBuffer buf;
	MQTTSFlags flags = {0};

	FUNC_ENTRY;
	header.len = 6 + strlen(clientID); 
	header.type = MQTTS_CONNECT;
	
	buf = MQTTSPacketSerialize_header(header);
	
	flags.cleanSession = cleansession;
	flags.will = will ? 1 : 0;
	writeChar(&buf.ptr, flags.all);
	writeChar(&buf.ptr, 0x01); /* protocol ID */
	writeInt(&buf.ptr, keepAlive);
	memcpy(buf.ptr, clientID, strlen(clientID));

	FUNC_EXIT;
	return buf;
}


PacketBuffer MQTTSSerialize_connack(int returnCode)
{
	MQTTSHeader header;
	PacketBuffer buf;

	FUNC_ENTRY;
	header.len = 3;
	header.type = MQTTS_CONNACK;
	
	buf = MQTTSPacketSerialize_header(header);
	*(buf.ptr)++ = (char)returnCode;
	
	FUNC_EXIT;
	return buf;
}

/*
PacketBuffer MQTTSSerialize_regAck(int msgId, int topicId, char returnCode)
{
	MQTTS_RegAck packet;
	int rc = 0;
	char *buf, *ptr;
	int datalen = 5;

	FUNC_ENTRY;
	packet.header.len = 7;
	packet.header.type = MQTTS_REGACK;

	ptr = buf = malloc(datalen);
	writeInt(&ptr, topicId);
	writeInt(&ptr, msgId);
	writeChar(&ptr, returnCode);

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, datalen);
	free(buf);

	FUNC_EXIT_RC(rc);
	return rc;
}
*/
