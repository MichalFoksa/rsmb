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


#if !defined(PERSISTENCE_H)
#define PERSISTENCE_H

#include "Broker.h"
#include <stdio.h>

int Persistence_read_config(char* filename, BrokerStates* s, int config_set);
void Persistence_free_config(BrokerStates* bs);

FILE* Persistence_open_retained(char mode);
int Persistence_write_retained(char* payload, int payloadlen, int qos, char* topicName);
RetainedPublications* Persistence_read_retained();

FILE* Persistence_open_subscriptions(char mode);
int Persistence_write_subscription(Subscriptions*);
Subscriptions* Persistence_read_subscription();
void Persistence_close_file();

void Persistence_read_command(BrokerStates* bs);

#endif /* PERSISTENCE_H */
