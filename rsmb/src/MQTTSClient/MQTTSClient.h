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
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

/*
 * Calls:
 *
 * 	- MQTTS_search(short radius)
 *      can get multiple replies
 *      no connection needed
 *
 *    MQTTS_connect
 *    	gateway sends willtopicreq and willtopicmsg, so replies to these packets can be handled
 *      internally by the client.  The connect options can thus include will details as for MQTT
 *
 *  - MQTTS_register(char* topicName)
 *
 *  - MQTTS_publish
 *       1. client object, flags, topic, payload (in the case of QoS 0, 1 or 2)
 *       2. address, flags, topic, payload (in the case of QoS -1, or broadcasting)
 *
 *  - MQTTS_subscribe
 *
 *  - MQTTS_unsubscribe
 *
 *  - MQTTS_willtopicupd
 *
 *  - MQTTS_willmsgupd
 *
 *  - MQTTS_disconnect
 *
 *  - synchronous information retrieval
 *      MQTTS_getGatewayInfo returns gateway id and address of next gateway
 *      MQTTS_receive
 *      MQTTS_waitForCompletion
 *      MQTTS_isConnected
 *
 *  - callbacks
 *      typedef MQTTS_gatewayInfo
 *      typedef int MQTTS_messageArrived
 *      typedef void MQTTS_deliveryComplete
 *      typedef void MQTTS_connectionLost
 *
 */
 
#if !defined(MQTTSCLIENT_H)
#define MQTTSCLIENT_H

#if defined(WIN32)
  #define DLLImport __declspec(dllimport)
  #define DLLExport __declspec(dllexport)
#else
  #define DLLImport extern
  #define DLLExport
#endif

DLLExport int MQTTSClient_create(MQTTClient* handle, char* serverURI, char* clientId, 
    int persistence_type, void* persistence_context);
    
DLLExport int MQTTSClient_connect(MQTTSClient handle, MQTTSClient_connectOptions* options);
