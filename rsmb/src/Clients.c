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

/**
 * @file
 * client utility functions
 */

#include "Clients.h"

#include <string.h>
#include <stdio.h>


/**
 * List callback function for comparing clients by clientid
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
#include "StackTrace.h"

int clientIDCompare(void* a, void* b, int value)
{
	char* as = ((Clients*)a)->clientID;
	char* bs = (value) ? ((Clients*)b)->clientID : (char*)b;

	return strcmp(as, bs);
}


/**
 * List callback function for comparing clients by socket
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int clientSocketCompare(void* a, void* b, int value)
{
	int ai = ((Clients*)a)->socket;
	int bi = (value) ? ((Clients*)b)->socket : *(int*)b;

	return (ai > bi) ? -1 : (ai == bi) ? 0 : 1;
}


int queuedMsgsCount(Clients* aClient)
{
	int i,
	    count = 0;

	for (i = 0; i < PRIORITY_MAX; ++i)
		count += aClient->queuedMsgs[i]->count;
	return count;
}


#if defined(MQTTS)
int clientAddrCompare(void* a, void* b, int value)
{
	//Clients* client = (Clients*)a;
	
	/* if client->addr is NULL (which it can be, for Bridge clients for example), return false. */
	//return (client->addr) ? strcmp(client->addr, (char*)b) == 0 : 0;

	char* as = ((Clients*)a)->addr;
	char* bs = (value) ? ((Clients*)b)->addr : (char*)b;

	return strcmp(as, bs);
}
#endif
