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

#ifndef MQTTSPROTOCOL_H_
#define MQTTSPROTOCOL_H_

#include "Clients.h"
#include "Broker.h"

#if !defined(NO_BRIDGE)
	#include "MQTTSProtocolOut.h"
#endif


int MQTTSProtocol_initialize(BrokerStates* aBrokerState);
void MQTTSProtocol_timeslice(int sock);

int MQTTSProtocol_handleAdvertises(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleSearchGws(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleGwInfos(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleConnects(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillTopics(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillMsgs(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleRegisters(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleRegacks(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePublishes(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePubacks(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePubcomps(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePubrecs(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePubrels(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleSubscribes(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleUnsubscribes(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleUnsubacks(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePingreqs(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handlePingresps(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleDisconnects(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillTopicUpds(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillTopicResps(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillMsgUpds(void* pack, int sock, char* clientAddr);
int MQTTSProtocol_handleWillMsgResps(void* pack, int sock, char* clientAddr);


char* MQTTSProtocol_getRegisteredTopicName(Clients* client, int topicId);
void MQTTSProtocol_freeRegistrationList(List* regList);
void MQTTSProtocol_emptyRegistrationList(List* regList);
int MQTTSProtocol_startPublishCommon(Clients* client, Publish* mqttPublish, int dup, int qos, int retained);
int MQTTSProtocol_startRegistration(Clients* client, char* topic);
Registration* MQTTSProtocol_registerTopic(Clients* client, char* topicName);
int MQTTSProtocol_getRegisteredTopicId(Clients* client, char* topicName);

#endif /* MQTTSPROTOCOL_H_ */
