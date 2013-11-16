/*******************************************************************************
 * Copyright (c) 2009, 2013 IBM Corp.
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

#ifndef PROTOCOL_H_
#define PROTOCOL_H_

void Protocol_timeslice();
void Protocol_processPublication(Publish* publish, char* originator);
int Protocol_startOrQueuePublish(Clients* pubclient, Publish* publish, int qos, int retained, int priority, Messages** m);

int Protocol_initialize(BrokerStates* bs);
void Protocol_terminate();
Clients* Protocol_getclientbyaddr(char* addr);
int clientPrefixCompare(void* prefix, void* clientid);
int Protocol_isClientQuiescing(Clients* client);
int Protocol_inProcess(Clients* client);

#if !defined(NO_BRIDGE)
Clients* Protocol_getoutboundclient(int sock);
#endif

int Protocol_handlePublishes(Publish* publish, int sock, Clients* client, char* clientid);

#endif /* PROTOCOL_H_ */
