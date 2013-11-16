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

#include "MQTTSPacket.h"
#include "Log.h"
#include "Clients.h"
#include "Messages.h"
#include "Protocol.h"
#include "Socket.h"
#include "StackTrace.h"

#include <stdlib.h>
#include <string.h>

#include "Heap.h"


static char* packet_names[] =
{
		"ADVERTISE", "SEARCHGW", "GWINFO", "RESERVED", "CONNECT", "CONNACK",
		"WILLTOPICREQ", "WILLTOPIC", "WILLMSGREQ", "WILLMSG", "REGISTER", "REGACK",
		"PUBLISH", "PUBACK", "PUBCOMP", "PUBREC", "PUBREL", "RESERVED",
		"SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "PINGREQ", "PINGRESP",
		"DISCONNECT", "RESERVED", "WILLTOPICUPD", "WILLTOPICRESP", "WILLMSGUPD",
		"WILLMSGRESP"
};

typedef void* (*mqtts_pf)(MQTTSHeader, char*);

typedef void (*mqtts_fpf)(void *);

static mqtts_pf new_mqtts_packets[] =
{
	MQTTSPacket_advertise, /* ADVERTISE */
	MQTTSPacket_searchGw,  /* SEARCHGW */
	MQTTSPacket_gwInfo,    /* GWINFO */
	NULL,                  /* RESERVED */
	MQTTSPacket_connect,   /* CONNECT */
	MQTTSPacket_connack,   /* CONNACK */
	MQTTSPacket_header_only, /* WILLTOPICREQ */
	MQTTSPacket_willTopic, /* WILLTOPIC */
	MQTTSPacket_header_only, /* WILLMSGREQ */
	MQTTSPacket_willMsg, /* WILLMSG */
	MQTTSPacket_register, /* REGISTER */
	MQTTSPacket_ack_with_reasoncode, /* REGACK */
	MQTTSPacket_publish, /* PUBLISH */
	MQTTSPacket_ack_with_reasoncode, /* PUBACK */
	MQTTSPacket_header_with_msgId, /* PUBCOMP */
	MQTTSPacket_header_with_msgId, /* PUBREC */
	MQTTSPacket_header_with_msgId, /* PUBREL */
	NULL,         /* RESERVED */
	MQTTSPacket_subscribe, /* SUBSCRIBE */
	MQTTSPacket_suback, /* SUBACK */
	MQTTSPacket_subscribe, /* UNSUBSCRIBE */
	MQTTSPacket_header_with_msgId, /* UNSUBACK */
	MQTTSPacket_pingreq, /* PINGREQ */
	MQTTSPacket_header_only, /* PINGRESP */
	MQTTSPacket_disconnect, /* DISCONNECT */
	NULL,         /* RESERVED */
	MQTTSPacket_willTopicUpd, /* WILLTOPICUPD */
	MQTTSPacket_header_only, /* WILLTOPICRESP */
	MQTTSPacket_willMsgUpd, /* WILLMSGUPD */
	MQTTSPacket_header_only  /* WILLMSGRESP */
};

static mqtts_fpf free_mqtts_packets[] =
{
	NULL, /* ADVERTISE */
	NULL,  /* SEARCHGW */
	MQTTSPacket_free_gwInfo,    /* GWINFO *** */
	NULL,                  /* RESERVED */
	MQTTSPacket_free_connect,   /* CONNECT *** */
	NULL,   /* CONNACK */
	NULL, /* WILLTOPICREQ */
	MQTTSPacket_free_willTopic, /* WILLTOPIC *** */
	NULL, /* WILLMSGREQ */
	MQTTSPacket_free_willMsg, /* WILLMSG *** */
	MQTTSPacket_free_register, /* REGISTER *** */
	NULL, /* REGACK */
	MQTTSPacket_free_publish, /* PUBLISH *** */
	NULL, /* PUBACK */
	NULL, /* PUBCOMP */
	NULL, /* PUBREC */
	NULL, /* PUBREL */
	NULL,         /* RESERVED */
	MQTTSPacket_free_subscribe, /* SUBSCRIBE *** */
	NULL, /* SUBACK */
	MQTTSPacket_free_subscribe, /* UNSUBSCRIBE *** */
	NULL, /* UNSUBACK */
	MQTTSPacket_free_pingreq, /* PINGREQ *** */
	NULL, /* PINGRESP */
	NULL, /* DISCONNECT */
	NULL,         /* RESERVED */
	MQTTSPacket_free_willTopicUpd, /* WILLTOPICUPD *** */
	NULL, /* WILLTOPICRESP */
	MQTTSPacket_free_willMsgUpd, /* WILLMSGUPD *** */
	NULL  /* WILLMSGRESP */
};


char* MQTTSPacket_name(int ptype)
{
	return (ptype >= 0 && ptype <= MQTTS_WILLMSGRESP) ? packet_names[ptype] : "UNKNOWN";
}

void* MQTTSPacket_Factory(int sock, char** clientAddr, int* error)
{
	char* data = NULL;
	static MQTTSHeader header;
	int ptype;
	void* pack = NULL;

	struct sockaddr_in cliAddr;
	int n;
	char msg[512];
	socklen_t len = sizeof(cliAddr);

	FUNC_ENTRY;
/* #if !defined(NO_BRIDGE)
	client = Protocol_getoutboundclient(sock);
	FUNC_ENTRY;
	if (client!=NULL)
		n = recv(sock,msg,512,0);
	else
 #endif */
	n = recvfrom(sock, msg, 512, 0, (struct sockaddr *)&cliAddr, &len );
	if (n == SOCKET_ERROR)
	{
		int en = Socket_error("udp read error",sock);
		if (en == EINVAL)
			Log(LOG_WARNING, 0, "EINVAL");

		*error = SOCKET_ERROR;
		goto exit;
	}

	*clientAddr = Socket_getaddrname((struct sockaddr *)&cliAddr, sock);
/*
	printf("%d bytes of data on socket %d from %s\n",n,sock,*clientAddr);
	if (n>0) {
		for (i=0;i<n;i++) {
			printf("%d ",msg[i]);
		}
		printf("\n");
	}
*/
	*error = SOCKET_ERROR;  // indicate whether an error occurred, or not
	if (n < 2)
		goto exit;

    header.len = msg[0];
	header.type = msg[1];
    data = &(msg[2]);

    if (header.len != n)
    {
		*error = UDPSOCKET_INCOMPLETE;
		goto exit;
    }
	else
	{
		ptype = header.type;
		if (ptype < MQTTS_ADVERTISE || ptype > MQTTS_WILLMSGRESP || new_mqtts_packets[ptype] == NULL)
			Log(TRACE_MAX, 17, NULL, ptype);
		else
		{
			if ((pack = (*new_mqtts_packets[ptype])(header, data)) == NULL)
			{
				*error = BAD_MQTTS_PACKET;
			}
		}
	}
exit:
   	FUNC_EXIT_RC(*error);
   	return pack;
}


void* MQTTSPacket_header_only(MQTTSHeader header, char* data)
{
	MQTTS_Header* pack = NULL;

	FUNC_ENTRY;
	if (header.len != 2)
		goto exit;
	pack = malloc(sizeof(MQTTS_Header));
	pack->header = header;
exit:
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_header_with_msgId(MQTTSHeader header, char* data)
{
	MQTTS_Header_MsgId* pack = NULL;
	char* curdata = data;

	FUNC_ENTRY;
	if (header.len != 4)
		goto exit;
	pack = malloc(sizeof(MQTTS_Header_MsgId));
	pack->header = header;
	pack->msgId = readInt(&curdata);
exit:
	FUNC_EXIT;
	return pack;
}


void* MQTTSPacket_ack_with_reasoncode(MQTTSHeader header, char* data)
{
	MQTTS_Ack* pack = NULL;
	char* curdata = data;

	FUNC_ENTRY;
	if (header.len != 7)
		goto exit;
	pack = malloc(sizeof(MQTTS_Ack));
	pack->header = header;
	pack->topicId = readInt(&curdata);
	pack->msgId = readInt(&curdata);
	pack->returnCode = readChar(&curdata);
exit:
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_advertise(MQTTSHeader header, char* data)
{
	MQTTS_Advertise* pack = malloc(sizeof(MQTTS_Advertise));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->gwId = readChar(&curdata);
	pack->duration = readInt(&curdata);
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_searchGw(MQTTSHeader header, char* data)
{
	MQTTS_SearchGW* pack = malloc(sizeof(MQTTS_SearchGW));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->radius = readChar(&curdata);
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_gwInfo(MQTTSHeader header, char* data)
{
	MQTTS_GWInfo* pack = malloc(sizeof(MQTTS_GWInfo));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->gwId = readChar(&curdata);
	if (header.len > 3)
	{
		pack->gwAdd = malloc(header.len-2);
		memcpy(pack->gwAdd, curdata, header.len-3);
		pack->gwAdd[header.len-3] = '\0';
	}
	else
	{
		pack->gwAdd = 0;
	}
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_connect(MQTTSHeader header, char* data)
{
	MQTTS_Connect* pack = malloc(sizeof(MQTTS_Connect));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->flags.all = readChar(&curdata);
	pack->protocolID = readChar(&curdata);
	pack->keepAlive = readInt(&curdata);
	pack->clientID = malloc(header.len-5);
	memcpy(pack->clientID, curdata, header.len-6);
	pack->clientID[header.len-6] = '\0';
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_connack(MQTTSHeader header, char* data)
{
	MQTTS_Connack* pack = malloc(sizeof(MQTTS_Connack));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->returnCode = readChar(&curdata);
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_willTopic(MQTTSHeader header, char* data)
{
	MQTTS_WillTopic* pack = malloc(sizeof(MQTTS_WillTopic));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->flags.all = readChar(&curdata);
	pack->willTopic = malloc(header.len-2);
	memcpy(pack->willTopic, curdata, header.len-3);
	pack->willTopic[header.len-3] = '\0';
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_willMsg(MQTTSHeader header, char* data)
{
	MQTTS_WillMsg* pack = malloc(sizeof(MQTTS_WillMsg));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->willMsg = malloc(header.len-1);
	memcpy(pack->willMsg, curdata, header.len-2);
	pack->willMsg[header.len-2] = '\0';
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_register(MQTTSHeader header, char* data)
{
	MQTTS_Register* pack = malloc(sizeof(MQTTS_Register));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->topicId = readInt(&curdata);
	pack->msgId = readInt(&curdata);
	pack->topicName = malloc(header.len-5);
	memcpy(pack->topicName, curdata, header.len-6);
	pack->topicName[header.len-6] = '\0';
	FUNC_EXIT;
	return pack;
}


void* MQTTSPacket_publish(MQTTSHeader header, char* data)
{
	MQTTS_Publish* pack = malloc(sizeof(MQTTS_Publish));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->flags.all = readChar(&curdata);
	if (pack->flags.topicIdType == MQTTS_TOPIC_TYPE_SHORT)
	{
		char* st = malloc(3);
		st[0] = readChar(&curdata);
		st[1] = readChar(&curdata);
		st[2] = '\0';
		pack->topicId = 0;
		pack->shortTopic = st;
	}
	else
	{
		pack->topicId = readInt(&curdata);
		pack->shortTopic = NULL;
	}

	pack->msgId = readInt(&curdata);
	pack->data = malloc(header.len - 7);
	memcpy(pack->data, curdata, header.len - 7);
	pack->dataLen = header.len - 7;
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_subscribe(MQTTSHeader header, char* data)
{
	MQTTS_Subscribe* pack = malloc(sizeof(MQTTS_Subscribe));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->flags.all = readChar(&curdata);
	pack->msgId = readInt(&curdata);
	if (pack->flags.topicIdType == 0x01)
	{
		pack->topicId = readInt(&curdata);
		pack->topicName = NULL;
	}
	else
	{
		pack->topicId = 0;
		pack->topicName = malloc(header.len-4);
		memcpy(pack->topicName, curdata, header.len-5);
		pack->topicName[header.len-5] = '\0';
	}
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_suback(MQTTSHeader header, char* data)
{
	MQTTS_SubAck* pack = malloc(sizeof(MQTTS_SubAck));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->flags.all = readChar(&curdata);
	pack->topicId = readInt(&curdata);
	pack->msgId = readInt(&curdata);
	pack->returnCode = readChar(&curdata);
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_pingreq(MQTTSHeader header, char* data)
{
	MQTTS_PingReq* pack = malloc(sizeof(MQTTS_PingReq));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	if (header.len > 2)
	{
		pack->clientId = malloc(header.len - 1);
		memcpy(pack->clientId, curdata, header.len - 2);
		pack->clientId[header.len-2] = '\0';
	}
	else
		pack->clientId = NULL;
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_disconnect(MQTTSHeader header, char* data)
{
	MQTTS_Disconnect* pack = malloc(sizeof(MQTTS_Disconnect));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	if (header.len > 2)
		pack->duration = readInt(&curdata);
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_willTopicUpd(MQTTSHeader header, char* data)
{
	MQTTS_WillTopicUpd* pack = malloc(sizeof(MQTTS_WillTopicUpd));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	if (header.len > 2)
	{
		pack->flags.all = readChar(&curdata);
		pack->willTopic = malloc(header.len - 2);
		memcpy(pack->willTopic, curdata, header.len - 3);
		pack->willTopic[header.len-3] = '\0';
	}
	else
	{
		pack->flags.all = 0;
		pack->willTopic = NULL;
	}
	FUNC_EXIT;
	return pack;
}

void* MQTTSPacket_willMsgUpd(MQTTSHeader header, char* data)
{
	MQTTS_WillMsgUpd* pack = malloc(sizeof(MQTTS_WillMsgUpd));
	char* curdata = data;

	FUNC_ENTRY;
	pack->header = header;
	pack->willMsg = malloc(header.len - 1);
	memcpy(pack->willMsg, curdata, header.len - 2);
	pack->willMsg[header.len-2] = '\0';
	FUNC_EXIT;
	return pack;
}


void MQTTSPacket_free_packet(MQTTS_Header* pack)
{
	FUNC_ENTRY;
	if (free_mqtts_packets[(int)pack->header.type] != NULL)
		(*free_mqtts_packets[(int)pack->header.type])(pack);
	free(pack);
	FUNC_EXIT;
}
void MQTTSPacket_free_gwInfo(void* pack)
{
	MQTTS_GWInfo* gwi = (MQTTS_GWInfo*)pack;

	FUNC_ENTRY;
	if (gwi->gwAdd != NULL)
		free(gwi->gwAdd);
	FUNC_EXIT;
}
void MQTTSPacket_free_connect(void* pack)
{
	MQTTS_Connect* con = (MQTTS_Connect*)pack;

	FUNC_ENTRY;
	if (con->clientID != NULL)
		free(con->clientID);
	FUNC_EXIT;
}
void MQTTSPacket_free_willTopic(void* pack)
{
	MQTTS_WillTopic* wt = (MQTTS_WillTopic*)pack;

	FUNC_ENTRY;
	if (wt->willTopic != NULL)
		free(wt->willTopic);
	FUNC_EXIT;
}
void MQTTSPacket_free_willMsg(void* pack)
{
	MQTTS_WillMsg* wm = (MQTTS_WillMsg*)pack;

	FUNC_ENTRY;
	if (wm->willMsg != NULL)
		free(wm->willMsg);
	FUNC_EXIT;
}
void MQTTSPacket_free_register(void* pack)
{
	MQTTS_Register* reg = (MQTTS_Register*)pack;

	FUNC_ENTRY;
	if (reg->topicName != NULL)
		free(reg->topicName);
	FUNC_EXIT;
}
void MQTTSPacket_free_publish(void* pack)
{
	MQTTS_Publish* pub = (MQTTS_Publish*)pack;

	FUNC_ENTRY;
	if (pub->data != NULL)
		free(pub->data);
	if (pub->shortTopic != NULL)
		free(pub->shortTopic);
	FUNC_EXIT;
}
void MQTTSPacket_free_subscribe(void* pack)
{
	MQTTS_Subscribe* sub = (MQTTS_Subscribe*)pack;

	FUNC_ENTRY;
	if (sub->topicName != NULL) {
		free(sub->topicName);
	}
	FUNC_EXIT;
}
void MQTTSPacket_free_pingreq(void* pack)
{
	MQTTS_PingReq* ping = (MQTTS_PingReq*)pack;

	FUNC_ENTRY;
	if (ping->clientId != NULL)
		free(ping->clientId);
	FUNC_EXIT;
}
void MQTTSPacket_free_willTopicUpd(void* pack)
{
	MQTTS_WillTopicUpd* wtu = (MQTTS_WillTopicUpd*)pack;

	FUNC_ENTRY;
	if (wtu->willTopic != NULL)
		free(wtu->willTopic);
	FUNC_EXIT;
}
void MQTTSPacket_free_willMsgUpd(void* pack)
{
	MQTTS_WillMsgUpd* wmu = (MQTTS_WillMsgUpd*)pack;

	FUNC_ENTRY;
	if (wmu->willMsg != NULL)
		free(wmu->willMsg);
	FUNC_EXIT;
}

int MQTTSPacket_send(int socket, char* addr, MQTTSHeader header, char* buffer, int buflen)
{
	struct sockaddr_in cliaddr;
	int rc = 0;
	char *p;
	char *tmpAddr = malloc(strlen(addr));
	char *data = malloc(buflen+2);

	FUNC_ENTRY;
	data[0] = header.len;
	data[1] = header.type;
	memcpy(&(data[2]),buffer,buflen);
	strcpy(tmpAddr,addr);
	p = strtok(tmpAddr,":");
	memset(&cliaddr, 0, sizeof(cliaddr));
	cliaddr.sin_family = AF_INET;
	cliaddr.sin_addr.s_addr = inet_addr(p);
	cliaddr.sin_port = htons(atoi(strtok(NULL,":")));

/* #if !defined(NO_BRIDGE)
	client = Protocol_getoutboundclient(socket);
	FUNC_ENTRY;
	if (client != NULL)
	{
		rc = send(socket,data,buflen+2,0);
		printf("send returned %d\n",rc);
	}
	else
# endif */
		rc = sendto(socket,data,buflen+2,0,(const struct sockaddr*)&cliaddr,sizeof(cliaddr));

	if (rc == SOCKET_ERROR)
	{
		Socket_error("sendto", socket);
/*		if (err == EWOULDBLOCK || err == EAGAIN)
			rc = TCPSOCKET_INTERRUPTED;
*/

	}
	else
	{
		rc = 0;
	}


	/*
	printf("Send to %s on socket %d\n",addr,socket);
	printf("%u:",header.len);
	printf("%u",header.type);
	if (buffer != NULL)
	{
		for (i=0;i<buflen;i++)
		{
			printf(":%u",buffer[i]);
		}
	}
	printf("\n");
	*/
	free(tmpAddr);
	free(data);
    /*
	buf = malloc(10);
	buf[0] = header.byte;
	rc = 1 + MQTTPacket_encode(&buf[1], buflen);
	rc = Socket_putdatas(socket, buf, rc, 1, &buffer, &buflen);
	if (rc != TCPSOCKET_INTERRUPTED)
	  free(buf);
    */

	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_ack(Clients* client, char type)
{
	MQTTS_Header packet;
	int rc = 0;

	FUNC_ENTRY;
	packet.header.len = 2;
	packet.header.type = type;
	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, NULL, 0);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_ack_with_msgId(Clients* client, char type, int msgId)
{
	int rc = 0;
	char *buf, *ptr;
	MQTTS_Header_MsgId packet;

	FUNC_ENTRY;
	packet.header.len = 4;
	packet.header.type = type;
	ptr = buf = malloc(2);
	writeInt(&ptr,msgId);
	rc = MQTTSPacket_send(client->socket,client->addr, packet.header, buf,2);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_connack(Clients* client, int returnCode)
{
	int rc = 0;
	MQTTS_Connack packet;

	FUNC_ENTRY;
	packet.header.len = 3;
	packet.header.type = MQTTS_CONNACK;
	packet.returnCode = returnCode;
	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, &(packet.returnCode), 1);
	FUNC_EXIT;
	return rc;
}

int MQTTSPacket_send_willTopicReq(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_WILLTOPICREQ);
}

int MQTTSPacket_send_willMsgReq(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_WILLMSGREQ);
}

int MQTTSPacket_send_pingResp(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_PINGRESP);
}
int MQTTSPacket_send_willTopicResp(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_WILLTOPICRESP);
}
int MQTTSPacket_send_willMsgResp(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_WILLMSGRESP);
}

int MQTTSPacket_send_regAck(Clients* client, int msgId, int topicId, char returnCode)
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
int MQTTSPacket_send_subAck(Clients* client, MQTTS_Subscribe* sub, int topicId, int qos, char returnCode)
{
	MQTTS_SubAck packet;
	int rc = 0;
	char *buf, *ptr;
	int datalen = 6;

	FUNC_ENTRY;
	packet.header.len = 8;
	packet.header.type = MQTTS_SUBACK;

	ptr = buf = malloc(datalen);
	packet.flags.QoS = qos;
	writeChar(&ptr,packet.flags.all);
	writeInt(&ptr,topicId);
	writeInt(&ptr,sub->msgId);
	writeChar(&ptr,returnCode);
	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, datalen);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_unsubAck(Clients* client, int msgId)
{
	MQTTS_UnsubAck packet;
	int rc = 0;
	char *buf, *ptr;

	FUNC_ENTRY;
	packet.header.len = 4;
	packet.header.type = MQTTS_UNSUBACK;
	ptr = buf = malloc(2);
	writeInt(&ptr,msgId);
	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, 2);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_publish(Clients* client, MQTTS_Publish* pub)
{

	int rc = 0;
	char *buf, *ptr;
	int datalen = 5 + pub->dataLen;

	FUNC_ENTRY;
	ptr = buf = malloc(datalen);

	writeChar(&ptr,pub->flags.all);
	if (pub->flags.topicIdType == MQTTS_TOPIC_TYPE_SHORT)
	{
		writeChar(&ptr,pub->shortTopic[0]);
		writeChar(&ptr,pub->shortTopic[1]);
	}
	else
		writeInt(&ptr,pub->topicId);
	writeInt(&ptr,pub->msgId);
	memcpy(ptr,pub->data,pub->dataLen);
	rc = MQTTSPacket_send(client->socket, client->addr, pub->header, buf, datalen);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_puback(Clients* client, MQTTS_Publish* publish, char returnCode)
{
	MQTTS_PubAck packet;
	char *buf, *ptr;
	int rc = 0;
	int datalen = 5;

	FUNC_ENTRY;
	packet.header.len = 7;
	packet.header.type = MQTTS_PUBACK;

	ptr = buf = malloc(datalen);

	if ( publish->flags.topicIdType == MQTTS_TOPIC_TYPE_SHORT)
	{
		writeChar(&ptr,publish->shortTopic[0]);
		writeChar(&ptr,publish->shortTopic[1]);
	}
	else
	{
		writeInt(&ptr,publish->topicId);
	}
	writeInt(&ptr, publish->msgId);
	writeChar(&ptr, returnCode);

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, datalen);
	free(buf);

	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_pubrec(Clients* client, int msgId)
{
	return MQTTSPacket_send_ack_with_msgId(client, MQTTS_PUBREC, msgId);
}
int MQTTSPacket_send_pubrel(Clients* client, int msgId)
{
	return MQTTSPacket_send_ack_with_msgId(client, MQTTS_PUBREL, msgId);
}
int MQTTSPacket_send_pubcomp(Clients* client, int msgId)
{
	return MQTTSPacket_send_ack_with_msgId(client, MQTTS_PUBCOMP, msgId);
}


int MQTTSPacket_send_register(Clients* client, int topicId, char* topicName, int msgId)
{
	MQTTS_Register packet;
	int rc = 0;
	char *buf, *ptr;
	int datalen = 4 + strlen(topicName);

	FUNC_ENTRY;
	packet.header.len = datalen+2;
	packet.header.type = MQTTS_REGISTER;

	ptr = buf = malloc(datalen);

	writeInt(&ptr,topicId);
	writeInt(&ptr,msgId);
	memcpy(ptr,topicName,strlen(topicName));

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, datalen);
	free(buf);

	FUNC_EXIT_RC(rc);
	return rc;
}


/****************************************************************/
/* client/bridge specific sends                                 */
/****************************************************************/

int MQTTSPacket_send_connect(Clients* client)
{
	char *buf, *ptr;
	MQTTS_Connect packet;
	int rc, len;

	FUNC_ENTRY;
	len = 4 + strlen(client->clientID);

	ptr = buf = malloc(len);

	packet.header.type = MQTTS_CONNECT;
	packet.header.len = len+2;
	packet.flags.all = 0;
	packet.flags.cleanSession = client->cleansession;
	packet.flags.will = (client->will) ? 1 : 0;

	writeChar(&ptr, packet.flags.all);
	writeChar(&ptr, 0x01);
	writeInt(&ptr, client->keepAliveInterval);
	memcpy(ptr, client->clientID, len-4);

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, len);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_willTopic(Clients* client)
{
	char *buf, *ptr;
	MQTTS_WillTopic packet;
	int rc, len;

	FUNC_ENTRY;
	len = 1 + strlen(client->will->topic);

	ptr = buf = malloc(len);

	packet.header.type = MQTTS_WILLTOPIC;
	packet.header.len = len+2;
	packet.flags.all = 0;
	packet.flags.QoS = client->will->qos;
	packet.flags.retain = client->will->retained;

	writeChar(&ptr, packet.flags.all);
	memcpy(ptr, client->will->topic, len-1);

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, len);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}
int MQTTSPacket_send_willMsg(Clients* client)
{
	char *buf, *ptr;
	MQTTS_WillMsg packet;
	int rc, len;

	FUNC_ENTRY;
	len = strlen(client->will->msg);

	ptr = buf = malloc(len);

	packet.header.type = MQTTS_WILLMSG;
	packet.header.len = len+2;

	memcpy(ptr, client->will->msg, len);

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, len);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_pingReq(Clients* client)
{
	return MQTTSPacket_send_ack(client,MQTTS_PINGREQ);
}


int MQTTSPacket_send_disconnect(Clients* client, int duration)
{
	int rc = 0;

	FUNC_ENTRY;
	if (duration == 0)
		rc = MQTTSPacket_send_ack(client,MQTTS_DISCONNECT);
	/* TODO: when support for sleeping clients is added, need to handle duration > 0 */
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTSPacket_send_subscribe(Clients* client, char* topicName, int qos, int msgId)
{
	MQTTS_Subscribe packet;
	int rc = 0;
	char *buf, *ptr;
	int datalen = 3 + strlen(topicName);

	FUNC_ENTRY;
	packet.header.len = datalen+2;
	packet.header.type = MQTTS_SUBSCRIBE;

	/* TODO: support TOPIC_TYPE_PREDEFINED/TOPIC_TYPE_SHORT */
	packet.flags.all = 0;
	packet.flags.topicIdType = MQTTS_TOPIC_TYPE_NORMAL;
	packet.flags.QoS = qos;

	ptr = buf = malloc(datalen);

	writeChar(&ptr,packet.flags.all);
	writeInt(&ptr,msgId);
	memcpy(ptr,topicName,strlen(topicName));

	rc = MQTTSPacket_send(client->socket, client->addr, packet.header, buf, datalen);
	free(buf);
	FUNC_EXIT_RC(rc);
	return rc;
}


#if defined(MQTTSPACKET_TEST)

int main(int argc, char *argv[]) {
	HeapScan();

	MQTTS_Publish* pack = malloc(sizeof(MQTTS_Publish));
	pack->header.type = MQTTS_PUBLISH;
	pack->data = malloc(10);
	MQTTSPacket_free_packet((MQTTS_Header*)pack);

	Clients* client = malloc(sizeof(Clients));
	client->cleansession = 1;
	client->clientID = "a";
	client->keepAliveInterval = 15;
	/* MQTTSPacket_send_connect(client); */
	/*  7:4:4:1:0:15:97 */
	free(client);



	Heap_terminate();
	return 0;
}

#endif

#endif
