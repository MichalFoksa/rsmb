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
 *    Nicholas O'Leary, Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

#ifndef MQTTMPPROTOCOL_H_
#define MQTTMPPROTOCOL_H_

#include "Broker.h"

void MQTTMPProtocol_initialize(BrokerStates* bs);
void MQTTMPProtocol_timeslice(int sock);
void MQTTMPProtocol_terminate();
int MQTTMPProtocol_getNextVirtualSocket();
void MQTTMPProtocol_closeSession(Clients* client, int send_close);
int MQTTMPProtocol_get_real_socket(int sock);
int MQTTMPProtocol_getCurrentChannel();
Clients* MQTTMPProtocol_getClientForVirtualSocket(int virtSocket);
void MQTTMPProtocol_closeSocket(int socket);
int MQTTMPProtocol_closeChannel(int socket, int channel);
int MQTTMPProtocol_send_connack(int aRc, int socket, int channel, char* clientID);
#define MP_PACKET_MQTT 0
#define MP_PACKET_CLOSE 1

#endif /* MQTTMPPROTOCOL_H_ */
