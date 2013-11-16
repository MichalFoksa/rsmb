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

#if !defined(SUBSENGINE_H)
#define SUBSENGINE_H

#include "LinkedList.h"
#include "Tree.h"
/*BE
include "LinkedList"
include "Tree"
BE*/

/**
 * System topic prefix
 */
#define sysprefix "$SYS/"

/*BE
def SUBSCRIPTIONS
{
	n32 ptr STRING open "clientName"
	n32 ptr STRING open "topicName"
	n32 dec "qos"
	n32 map bool "noLocal"
	n32 map bool "durable"
	n32 dec "priority"
	n32 map bool "wildcards"
}
BE*/
enum
{
	PRIORITY_LOW,
	PRIORITY_NORMAL,
	PRIORITY_HIGH,
};
#if !defined(PRIORITY_MAX)
#define PRIORITY_MAX 3
#endif
/**
 * Data for each client subscription
 */
typedef struct
{
	char* clientName;	      /**< ID of client which holds this subscription */
	char* topicName;	      /**< the name of the topic, possible wildcarded */
	int qos;		          	/**< the Quality of Service of the subscription */
	unsigned int noLocal;	  /**< noLocal flag (for bridge use) */
	int durable;		        /**< so that the save process knows which ones to omit */
	int priority;           /**< priority of subscription */
	int wildcards;
} Subscriptions;


/*BE
def RETAINEDPUBLICATIONS
{
	n32 ptr STRING open "topicName"
	n32 dec "qos"
	n32 ptr DATA open "payload"
	n32 dec "payloadlen"
}
BE*/
/**
 * Retained message data
 */
typedef struct
{
	char* topicName;	        /**< topic name string, no wildcards */
	int qos;			            /**< quality of service */
	char* payload;		        /**< message content */
	unsigned int payloadlen;	/**< length of payload */
} RetainedPublications;

Subscriptions* Subscriptions_initialize(char*, char*, int, int, int, int);

/*BE

defList(SUBSCRIPTIONS)
defTree(RETAINEDPUBLICATIONS)
defTree(SUBSCRIPTIONSList)

def SUBSCRIPTIONENGINES
{
	n32 ptr SUBSCRIPTIONSListTree open "subs"
	n32 ptr SUBSCRIPTIONSList open "wsubs"
	n32 ptr RETAINEDPUBLICATIONSTree open "retaineds"
	n32 dec "retained_changes"
	struct
	{
		n32 ptr SUBSCRIPTIONSList open "system_subs"
		n32 ptr RETAINEDPUBLICATIONSTree open "system_retaineds"
	}
}
BE*/
/**
 * Subscription engine main state
 */
typedef struct
{
	Tree* subs;     		      /**< non-wildcard main subscriptions */
	List* wsubs;			        /**< main subscription list - wildcard subscriptions */
	Tree* retaineds;		      /**< main retained message list */
	int retained_changes;	    /**< flag to show whether changes have been made since last save */
	struct
	{
		List* subs;			        /**< system topics */
		Tree* retaineds;	      /**< system retained messages */
	} system;				          /**< system topic space */
} SubscriptionEngines;

SubscriptionEngines* SubscriptionEngines_initialize();
void SubscriptionEngines_save(SubscriptionEngines* se);
void SubscriptionEngines_terminate(SubscriptionEngines* se);

int SubscriptionEngines_subscribe(SubscriptionEngines*, char*, char*, int, int, int, int);
void SubscriptionEngines_unsubscribe(SubscriptionEngines*, char*, char*);
char* SubscriptionEngines_mostSpecific(char* topicA, char* topicB);
List* SubscriptionEngines_getSubscribers(SubscriptionEngines*, char* topic, char* clientID);

void SubscriptionEngines_setRetained(SubscriptionEngines* se, char* topicName, int qos, char* payload, unsigned int payloadlen);
List* SubscriptionEngines_getRetained(SubscriptionEngines* se, char* topicName);
void SubscriptionEngines_clearRetained(SubscriptionEngines* se, char* topicName);

#endif
