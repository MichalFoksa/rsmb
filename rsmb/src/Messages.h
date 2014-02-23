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


#if !defined(MESSAGES_H)
#define MESSAGES_H

/**
 * Number of messages in the file
 */
#if !defined(MQTTS)
#define MESSAGE_COUNT 103
#else
#define MESSAGE_COUNT 107
#endif

/**
 * Largest message number
 */
#if !defined(MQTTS)
#define MAX_MESSAGE_INDEX 153
#else
#define MAX_MESSAGE_INDEX 303
#endif

#include "Broker.h"

int Messages_initialize(BrokerStates*);
void Messages_terminate();

char* Messages_get(int, int);

#endif
