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


#if !defined(MQTTPROTOCOLCLIENT_H)
#define MQTTPROTOCOLCLIENT_H

#include "Broker.h"
#include "LinkedList.h"
#include "SubsEngine.h"
#include "MQTTPacket.h"
#include "Clients.h"
#include "Log.h"
#include "MQTTProtocol.h"
#include "Messages.h"

/**
 * MQTT protocol maximum message id - as defined in the spec.
 */
#define MAX_MSG_ID 65535

/**
 * MQTT protocol maximum client id length - as defined in the spec.
 */
#define MAX_CLIENTID_LEN 23

int MQTTProtocol_assignMsgId(Clients* client);
int MQTTProtocol_startPublish(Clients* pubclient, Publish* publish, int qos, int retained, Messages** m);
int MQTTProtocol_queuePublish(Clients* pubclient, Publish* publish, int qos, int retained, int priority, Messages** m);
int MQTTProtocol_startOrQueuePublish(Clients* pubclient, Publish* publish, int qos, int retained, int priority, Messages** m);
Messages* MQTTProtocol_createMessage(Publish* publish, Messages** mm, int qos, int retained);
Publications* MQTTProtocol_storePublication(Publish* publish, int* len);
int messageIDCompare(void* a, void* b);
int MQTTProtocol_assignMsgId(Clients* client);

int MQTTProtocol_handlePublishes(void* pack, int sock);
int MQTTProtocol_handlePubacks(void* pack, int sock);
int MQTTProtocol_handlePubrecs(void* pack, int sock);
int MQTTProtocol_handlePubrels(void* pack, int sock);
int MQTTProtocol_handlePubcomps(void* pack, int sock);

void MQTTProtocol_keepalive(time_t);
int MQTTProtocol_processQueued(Clients* client);
int MQTTProtocol_retry(time_t, int);
void MQTTProtocol_retries(time_t now, Clients* client);
void MQTTProtocol_freeClient(Clients* client);
void MQTTProtocol_removeQoS0Messages(List* msgList);
void MQTTProtocol_emptyMessageList(List* msgList);
void MQTTProtocol_freeMessageList(List* msgList);

#endif
