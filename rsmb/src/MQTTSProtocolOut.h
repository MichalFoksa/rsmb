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
 *    Ian Craggs, Nicholas O'Leary - initial API and implementation and/or initial documentation
 *******************************************************************************/

#ifndef MQTTSPROTOCOLOUT_H_
#define MQTTSPROTOCOLOUT_H_

#include "Broker.h"
#include "LinkedList.h"
#include "SubsEngine.h"
#include "MQTTSPacket.h"
#include "Clients.h"
#include "Log.h"
#include "Messages.h"
#include "MQTTSProtocol.h"


Clients* MQTTSProtocol_create_multicast(char* ip_address, char* clientID, int loopback);
Clients* MQTTSProtocol_connect(char* ip_address, char* clientID, int cleansession, int try_private, int keepalive, willMessages* willMessage);


int MQTTSProtocol_handleConnacks(void* pack, int sock, char* clientAddr, Clients* client);
void MQTTSProtocol_reconnect(char* ip_address, Clients* client);
int MQTTSProtocol_handleWillTopicReqs(void* pack, int sock, char* clientAddr, Clients* client);
int MQTTSProtocol_handleWillMsgReqs(void* pack, int sock, char* clientAddr, Clients* client);
int MQTTSProtocol_handleSubacks(void* pack, int sock, char* clientAddr, Clients* client);
int MQTTSProtocol_startSubscription(Clients* client, char* topic, int qos);
int MQTTSProtocol_startClientRegistration(Clients* client, char* topic);

#endif /* MQTTSPROTOCOLOUT_H_ */
