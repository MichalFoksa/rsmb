/*******************************************************************************
 * Copyright (c) 2012, 2013 IBM Corp.
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

#if !defined(MQTTSPACKETSERIALIZE_H)
#define MQTTSPACKETSERIALIZE_H


typedef struct
{
	int len;
	char* data;
	char* ptr; /* pointer to the next free space in the buffer */
} PacketBuffer;


PacketBuffer MQTTSPacketSerialize_ack(char type, int msgId);
PacketBuffer MQTTSPacketSerialize_advertise(unsigned char gateway_id, short duration);
PacketBuffer MQTTSPacketSerialize_connect(int cleansession, int will, char protocolID, short keepAlive, char* clientID);
PacketBuffer MQTTSSerialize_connack(int returnCode);


#endif
