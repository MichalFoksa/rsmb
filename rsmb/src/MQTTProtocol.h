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

#if !defined(MQTTPROTOCOL_H)
#define MQTTPROTOCOL_H

#include "Broker.h"
#include "LinkedList.h"
#include "SubsEngine.h"
#include "MQTTPacket.h"
#include "Clients.h"

#define MAX_MSG_ID 65535
#define MAX_CLIENTID_LEN 23
#define INTERNAL_CLIENTID "$SYS/INTERNAL/CLIENT"

/*BE
map CONNACK_RETURN_CODES
{
   "CONNACK_CONNECTION_ACCEPTED" .
   "CONNACK_UNACCEPTABLE_PROTOCOL_VERSION" .
   "CONNACK_IDENTIFIER_REJECTED" .
   "CONNACK_BROKER_UNAVAILABLE" .
   "CONNACK_BAD_USERNAME_OR_PASSWORD" .
   "CONNACK_NOT_AUTHORIZED" .
   "CONNACK_NONE_RECEIVED" 99
}
BE*/

enum connack_return_codes
{
	CONNACK_CONNECTION_ACCEPTED, CONNACK_UNACCEPTABLE_PROTOCOL_VERSION,
	CONNACK_IDENTIFIER_REJECTED, CONNACK_BROKER_UNAVAILABLE,
	CONNACK_BAD_USERNAME_OR_PASSWORD, CONNACK_NOT_AUTHORIZED, CONNACK_NONE_RECEIVED=99
};

/*BE

include "Clients"

def PENDING_WRITE
{
	n32 dec "socket"
	n32 ptr PUBLICATIONS open "publication"
	n32 ptr CLIENTS open "client"
}

defList(PUBLICATIONS)
defList(PENDING_WRITE)

def MQTTPROTOCOL
{
	PUBLICATIONSList "publications"
	PENDING_WRITEList "pending_writes"
}

BE*/

typedef struct
{
	int socket;
	Publications* p;
	Clients* client;
} pending_write;


typedef struct
{
	List publications;
	List pending_writes; /* for qos 0 writes not complete */
} MQTTProtocol;

MQTTProtocol* MQTTProtocol_getState();
int MQTTProtocol_reinitialize();
int MQTTProtocol_initialize(BrokerStates*);
void MQTTProtocol_shutdownclients(Tree* clients, int terminate);
void MQTTProtocol_shutdown(int terminate);
void MQTTProtocol_checkPendingWrites();
int MQTTProtocol_housekeeping(int more_work);
void MQTTProtocol_timeslice(int sock, Clients* client);
void MQTTProtocol_clean_clients(Tree* clients);
void MQTTProtocol_terminate();
void MQTTProtocol_closeSession(Clients* client, int unclean);
void MQTTProtocol_removeAllSubscriptions(char* clientID);
void MQTTProtocol_sys_publish(char* topic, char* string);

int MQTTProtocol_handleConnects(void* pack, int sock, Clients* client);
int MQTTProtocol_handlePingreqs(void* pack, int sock, Clients* client);
int MQTTProtocol_handleDisconnects(void* pack, int sock, Clients* client);
void MQTTProtocol_processRetaineds(Clients* client, char* topic, int qos, int priority);
int MQTTProtocol_handleSubscribes(void* pack, int sock, Clients* client);
int MQTTProtocol_handleUnsubscribes(void* pack, int sock, Clients* client);

#if defined(MQTTS)
void MQTTProtocol_setWillTopic(Clients* client, char* topic, int retained, int qos);
void MQTTProtocol_setWillMsg(Clients* client, char* msg);
void MQTTProtocol_clearWill(Clients* client);
#endif

#if defined(NO_BRIDGE)
	#include "MQTTProtocolClient.h"
#else
	#include "MQTTProtocolOut.h"
#endif

#endif
