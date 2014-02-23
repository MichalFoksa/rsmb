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

/**
 * @file
 * Subscription Engine
 *
 * All the means for handling subscriptions
 */

#include "SubsEngine.h"
#include "Topics.h"
#include "Log.h"
#include "Persistence.h"
#include "Messages.h"
#include "StackTrace.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(UNIT_TESTS)
#include "Heap.h"
#endif

/**
 * Initialize one subscription record
 * @param clientid the id of the client
 * @param topic the topic name
 * @param qos the MQTT Quality of Service
 * @param noLocal boolean - whether the subscription is "noLocal"
 * @param durable boolean - whether the subscription is to be persisted
 * @return pointer to the new subscription structure
 */
Subscriptions* Subscriptions_initialize(char* clientid, char* topic, int qos, int noLocal, int durable, int priority)
{
	Subscriptions* news = malloc(sizeof(Subscriptions));

	FUNC_ENTRY;
	news->clientName = clientid;
	news->topicName = topic;
	news->qos = qos;
	news->noLocal = noLocal;
	news->durable = durable;
	news->priority = priority;
	FUNC_EXIT;
	return news;
}


/**
 * Create and initialize a new subscription engine
 * @return pointer to the new subscription engine structure
 */
SubscriptionEngines* SubscriptionEngines_initialize()
{
	SubscriptionEngines* newse = malloc(sizeof(SubscriptionEngines));

	FUNC_ENTRY;
	newse->subs = ListInitialize();
	newse->retaineds = ListInitialize();
	newse->retained_changes = 0;
	newse->system.subs = ListInitialize();
	newse->system.retaineds = ListInitialize();

#if !defined(SUBSENGINE_UNIT_TESTS)
	if (Persistence_open_retained('r'))
	{
		RetainedPublications* r;
		while ((r = Persistence_read_retained()))
		{
			if ((!Topics_isValidName(r->topicName)) || Topics_hasWildcards(r->topicName) ||
				(strncmp(r->topicName, sysprefix, strlen(sysprefix)) == 0))
			{
				Log(LOG_INFO, 66, NULL, r->topicName);
				free(r->payload);
				free(r->topicName);
				free(r);
			}
			else
				ListAppend(newse->retaineds, r, sizeof(SubscriptionEngines)+4*sizeof(List));
		}
		Persistence_close_file();
	}
	if (Persistence_open_subscriptions('r'))
	{
		Subscriptions* s;
		while ((s = Persistence_read_subscription()))
			ListAppend(newse->subs, s, sizeof(Subscriptions)+strlen(s->clientName)+strlen(s->topicName));
		Persistence_close_file();
	}
#endif
	FUNC_EXIT;
	return newse;
}


/**
 * Save the retained message table to disk and/or free al the retained messages.
 * If neither free nor save flag is true, no work is done.
 * @param retaineds the list of retained messages
 * @param must_free whether to free them
 * @param must_save whether to save them
 * @return success or error code
 */
int saveOrFreeRetaineds(List* retaineds, int must_free, int must_save)
{
	ListElement *current = NULL;
	int rc = 0;

	FUNC_ENTRY;
	while (ListNextElement(retaineds, &current))
	{
		/* also, write retained messages to persistence */
		RetainedPublications* r = current->content;
		if (must_save)
		{
#if defined(SUBSENGINE_UNIT_TESTS)
			;
#else
			if (Persistence_write_retained(r->payload, r->payloadlen, r->qos, r->topicName) != 0)
				rc = -1;
#endif
		}
		if (must_free)
		{
			free(r->topicName);
			free(r->payload);
		}
	}
	if (must_free)
		ListFree(retaineds);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Save the subscription table to disk and/or free al the subscriptions.
 * If neither free nor save flag is true, no work is done.
 * @param subs the list of subscriptions
 * @param must_free whether to free them
 * @param must_save whether to save them
 * @return success or error code
 */
int saveOrFreeSubscriptions(List* subs, int must_free, int must_save)
{
	ListElement *current = NULL;
	int rc = 0;

	FUNC_ENTRY;
	while (ListNextElement(subs, &current))
	{
		/* also, write retained messages to persistence */
		Subscriptions* s = current->content;
		if (must_save && s->durable)
		{
#if defined(SUBSENGINE_UNIT_TESTS)
			;
#else
			if (Persistence_write_subscription(s) != 0)
				rc = -1;
#endif
		}
		if (must_free)
			free(s->topicName);
	}
	if (must_free)
		ListFree(subs);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Save the subscription engine state to persistence.
 * @param se pointer to a subscription engine state structure
 */
void SubscriptionEngines_save(SubscriptionEngines* se)
{
	FUNC_ENTRY;
#if !defined(SUBSENGINE_UNIT_TESTS)
	Persistence_open_retained('w');
	if (saveOrFreeRetaineds(se->retaineds, 0, 1) != 0)
		Log(LOG_WARNING, 147, NULL);
	Persistence_close_file();
	se->retained_changes = 0;

	Persistence_open_subscriptions('w');
	if (saveOrFreeSubscriptions(se->subs, 0, 1) != 0)
		Log(LOG_WARNING, 148, NULL);
	Persistence_close_file();
#endif
	FUNC_EXIT;
}


/**
 * Terminate the subscription engine
 * @param se pointer to the subscription engine state structure
 */
void SubscriptionEngines_terminate(SubscriptionEngines* se)
{
	FUNC_ENTRY;
	saveOrFreeRetaineds(se->retaineds, 1, 0);
	saveOrFreeRetaineds(se->system.retaineds, 1, 0);

	saveOrFreeSubscriptions(se->subs, 1, 0);
	saveOrFreeSubscriptions(se->system.subs, 1, 0);

	free(se);
	FUNC_EXIT;
}


/**
 * Make a subscription - common function internal to this module.
 * @param se pointer to the subscription engine state structure
 * @param sl subscription list to add to
 * @param aClientid the id of the client which is subscribing
 * @param aTopic a topic name string - can have wildcards
 * @param qos the MQTT Quality of Service
 * @param noLocal boolean - whether the subscription is "noLocal"
 * @param durable boolean - whether the subscription is to be persisted
 * @return flag indicating whether the subscription table has changed
 */
int SubscriptionEngines_subscribe1(SubscriptionEngines* se, List* sl, char* aClientid, char* aTopic, int qos, int noLocal, int durable, int priority)
{
	int changed = 0;
	ListElement *current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(sl, &current))
	{
		Subscriptions* s = current->content;
		if (strcmp(s->clientName, aClientid) == 0 && strcmp(s->topicName, aTopic) == 0)
		{
			Log(TRACE_MINIMUM, 21, NULL, aClientid, aTopic, qos);
			if (s->durable != durable || (durable && (s->qos != qos || s->noLocal != noLocal || s->priority != priority)))
				(se->retained_changes)++;
			if (s->durable != durable || s->qos != qos || s->noLocal != noLocal || s->priority != priority)
				changed = 1;
			free(s->topicName); /* make sure we free the old topic name, even though it is the same */
			s->topicName = aTopic; /* point to the new (same value) one */
			s->qos = qos;
			s->noLocal = noLocal;
			s->durable = durable;
			s->priority = priority;
			break;
		}
	}
	if (current == NULL)
	{
		Log(TRACE_MINIMUM, 22, NULL, aClientid, aTopic, qos);
		ListAppend(sl, Subscriptions_initialize(aClientid, aTopic, qos, noLocal, durable, priority), sizeof(Subscriptions));
		if (durable)
			(se->retained_changes)++;
		changed = 1;
	}
	FUNC_EXIT_RC(changed);
	return changed;
}


/**
 * Make a subscription
 * @param se pointer to the subscription engine state structure
 * @param aClientid the id of the client which is subscribing
 * @param aTopic a topic name string - can have wildcards
 * @param qos the MQTT Quality of Service
 * @param noLocal boolean - whether the subscription is "noLocal"
 * @param durable boolean - whether the subscription is to be persisted
 * @return flag indicating whether the subscription table has changed
 */
int SubscriptionEngines_subscribe(SubscriptionEngines* se, char* aClientid, char* aTopic, int qos, int noLocal, int durable, int priority)
{
	int changed = 0;

	FUNC_ENTRY;
	if (strncmp(aTopic, sysprefix, strlen(sysprefix)) == 0)
		changed = SubscriptionEngines_subscribe1(se, se->system.subs, aClientid, aTopic, qos, noLocal, durable, priority);
	else
		changed = SubscriptionEngines_subscribe1(se, se->subs, aClientid, aTopic, qos, noLocal, durable, priority);

	FUNC_EXIT_RC(changed);
	return changed;
}


/**
 * Try to remove a subscription
 * @param se pointer to the subscription engine state structure
 * @param sl subscription list to remove from
 * @param aClientid the id of the client which is subscribing
 * @param aTopic a topic name string - can have wildcards.  NULL means remove all subscriptions for the clientid
 */
void SubscriptionEngines_unsubscribe1(SubscriptionEngines* se, List* sl, char* aClientid, char* aTopic)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	ListNextElement(sl, &current);
	while (current)
	{
		Subscriptions* s = current->content;
		ListNextElement(sl, &current);
		if (strcmp(s->clientName, aClientid) == 0 && (aTopic == NULL || strcmp(s->topicName, aTopic) == 0))
		{
			Log(TRACE_MINIMUM, 23, NULL, s->clientName, s->topicName, s->qos);
			free(s->topicName);
			if (s->durable)
				(se->retained_changes)++;
			if (ListRemove(sl, s) == 0)
				Log(LOG_SEVERE, 0, "Failed to remove subscription %s from client %s", s->topicName, s->clientName);
			if (aTopic)
				break;
		}
	}
	FUNC_EXIT;
}


/**
 * Try to remove a subscription
 * @param se pointer to the subscription engine state structure
 * @param aClientid the id of the client which is subscribing
 * @param aTopic a topic name string - can have wildcards.  NULL means unsubscribe to all topics
 */
void SubscriptionEngines_unsubscribe(SubscriptionEngines* se, char* aClientid, char* aTopic)
{
	int issys = 0;

	FUNC_ENTRY;
	if (aTopic)
		issys = strncmp(aTopic, sysprefix, strlen(sysprefix)) == 0;
	if (aTopic == NULL || issys)
	{
		/* if (aTopic && strcmp(aTopic, "$SYS/#") == 0)
		 * aTopic = NULL;
		 */
		SubscriptionEngines_unsubscribe1(se, se->system.subs, aClientid, aTopic);
	}
	if (aTopic == NULL || !issys)
	{
		/* if (aTopic && strcmp(aTopic, (char*)MULTI_LEVEL_WILDCARD) == 0)
		 * 	aTopic = NULL;
		 */
		SubscriptionEngines_unsubscribe1(se, se->subs, aClientid, aTopic);
	}
	FUNC_EXIT;
}


/**
 * List search callback to compare subscriptions by client id
 * @param a first subscription
 * @param b second subscriptions
 * return boolean flag - compared ok?
 */
int subsClientIDCompare(void* a, void* b)
{
	Subscriptions* s = (Subscriptions*)a;

	return strcmp(s->clientName, (char*)b) == 0;
}


/**
 * Returns the "most specific" topic given two to compare.  The most specific topic is defined as
 * the topic with a wildcard character furthest into the topic string.  If they have wildcards at
 * the same position, + is more specific than #.  If they are both equal then it doesn't matter
 * which we pick.
 * @param topicA the first topic
 * @param topicB the second topic
 * @return pointer to the "most specific" topic
 */
char* SubscriptionEngines_mostSpecific(char* topicA, char* topicB)
{
	char* rc = topicB;
	int t1 = strcspn(topicA, "#+");
	int t2 = strcspn(topicB, "#+");

	if ((t1 == t2 && topicB[t1] == '#') || t2 < t1)
		rc = topicA;
	return rc;
}


/**
 * Internal function to find all the subscribers for a topic in any topic space
 * @param sl pointer to the subscription list for a topic space
 * @param aTopic a topic name string
 * @param clientID	the id of the client
 * @return a List of clients subscribed to the topic
 */
List* SubscriptionEngines_getSubscribers1(List* sl, char* aTopic, char* clientID)
{
	List* rc = ListInitialize(); /* list of subscription structures */
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(sl, &current))
	{
		Subscriptions* s = current->content;
		Log(TRACE_MINIMUM, 24, NULL, s->clientName, s->qos, s->topicName);
		if (Topics_matches(s->topicName, aTopic) &&
			((s->noLocal == 0) || (strcmp(s->clientName, clientID) != 0)))
		{
			rc->current = NULL;
			if (ListFindItem(rc, s->clientName, subsClientIDCompare))
			{ /* if we have already found a subscription for this clientid, determine which to use */
				Subscriptions* rcs = rc->current->content;
				/* determine which QoS to use */
				if (SubscriptionEngines_mostSpecific(rcs->topicName, s->topicName) == s->topicName)
				{
					rcs->qos = s->qos;
					rcs->priority = s->priority;
				}
			}
			else
			{ /* we don't already have a subscription for this client id, so we just add it to the list */
				Subscriptions* rcs = malloc(sizeof(Subscriptions));
				Log(TRACE_MINIMUM, 25, NULL, s->clientName);
				rcs->clientName = s->clientName;
				rcs->qos = s->qos;
				rcs->priority = s->priority;
				rcs->topicName = s->topicName;
				ListAppend(rc, rcs, sizeof(Subscriptions));
			}
		}
	}
	FUNC_EXIT;
	return rc;
}


/**
 * Find all the subscribers for a topic
 * @param se pointer to the subscription engine state structure
 * @param aTopic a topic name string
 * @param clientID	the id of the client
 * @return a List of clients subscribed to the topic
 */
List* SubscriptionEngines_getSubscribers(SubscriptionEngines* se, char* aTopic, char* clientID)
{
	List* rc = NULL;

	FUNC_ENTRY;
	if (strncmp(aTopic, sysprefix, strlen(sysprefix)) == 0)
		rc = SubscriptionEngines_getSubscribers1(se->system.subs, aTopic, clientID);
	else
		rc = SubscriptionEngines_getSubscribers1(se->subs, aTopic, clientID);
	FUNC_EXIT;
	return rc;
}


/**
 *	Set a retained publication in the normal or system topic space (internal to this module).
 *	@param rl the normal or system list of retained publications
 *	@param topicName the topic string on which to set the retained publication
 *	@param qos the quality of service
 *	@param payload the contents of the message
 *	@param payloadlen the length of payload
 */
void SubscriptionEngines_setRetained1(List* rl, char* topicName, int qos, char* payload, unsigned int payloadlen)
{
	RetainedPublications* found = NULL;

	FUNC_ENTRY;
	if (rl->current != NULL)
	{
		RetainedPublications* r = rl->current->content;
		if (strcmp(r->topicName, topicName) == 0)
			found = r;
	}
	if (found == NULL)
	{
		ListElement* current = NULL;
		while (ListNextElement(rl, &current))
		{
			RetainedPublications* r = current->content;
			if (strcmp(r->topicName, topicName) == 0)
			{
				found = r;
				break;
			}
		}
	}
	if (payloadlen == 0)
	{
		if (found != NULL)
		{
			/* remove current retained publication */
			free(found->topicName);
			free(found->payload);
			ListRemove(rl, found);
		}
		goto exit;
	}
	if (found == NULL)
	{
		found = malloc(sizeof(RetainedPublications));
		ListAppend(rl, found, sizeof(RetainedPublications));
		found->topicName = NULL;
		found->payload = NULL;
	}
	if (found->topicName != NULL)
	{
		if (strlen(topicName) != strlen(found->topicName))
			found->topicName = realloc(found->topicName, strlen(topicName)+1);
	}
	else
		found->topicName = malloc(strlen(topicName)+1);
	strcpy(found->topicName, topicName);
	found->qos = qos;
	if (found->payload != NULL)
	{
		if (payloadlen != found->payloadlen)
			found->payload = realloc(found->payload, payloadlen);
	}
	else
		found->payload = malloc(payloadlen);
	found->payloadlen = payloadlen;
	memcpy(found->payload, payload, payloadlen);
exit:
	FUNC_EXIT;
}


/**
 *	Set a retained publication for a subscription engine.
 *	@param se pointer to a subscription engine structure
 *	@param topicName the topic string on which to set the retained publication
 *	@param qos the quality of service
 *	@param payload the contents of the message
 *	@param payloadlen the length of payload
 */
void SubscriptionEngines_setRetained(SubscriptionEngines* se, char* topicName, int qos, char* payload, unsigned int payloadlen)
{
	FUNC_ENTRY;
	if (strncmp(topicName, sysprefix, strlen(sysprefix)) == 0)
		SubscriptionEngines_setRetained1(se->system.retaineds, topicName, qos, payload, payloadlen);
	else
	{
		(se->retained_changes)++;
		SubscriptionEngines_setRetained1(se->retaineds, topicName, qos, payload, payloadlen);
	}
	FUNC_EXIT;
}


/**
 * Internal function to return a retained publication.
 * @param retaineds the retained publication list to search
 * @param topicName the topic name string to search for
 * @return the list of matching retained publications
 */
List* SubscriptionEngines_getRetained1(List* retaineds, char* topicName)
{
	List* rc = ListInitialize(); /* list of RetainedPublication structures */
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(retaineds, &current))
	{
		RetainedPublications* r = current->content;
		Log(TRACE_MAX, 26, NULL, r->topicName, topicName);
		if (Topics_matches(topicName, r->topicName))
		{
			Log(TRACE_MAX, 27, NULL, r->topicName, topicName);
			ListAppend(rc, r, sizeof(RetainedPublications));
		}
	}
	FUNC_EXIT;
	return rc;
}


/**
 * Return a retained publication.
 * @param se pointer to a subscription engine structure
 * @param topicName the topic name string to search for
 * @return the list of matching retained publications
 */
List* SubscriptionEngines_getRetained(SubscriptionEngines* se, char* topicName)
{
	List* rc = NULL;

	FUNC_ENTRY;
	if (strncmp(topicName, sysprefix, strlen(sysprefix)) == 0)
		rc = SubscriptionEngines_getRetained1(se->system.retaineds, topicName);
	else
		rc =  SubscriptionEngines_getRetained1(se->retaineds, topicName);
	FUNC_EXIT;
	return rc;
}


/**
 *	Clear retained publications for a topic set in a subscription engine.
 *	@param se pointer to a subscription engine structure
 *	@param topicName the topic string on which to clear the retained publications.  Can include
 *		wildcards, but not be a system topic.
 */
void SubscriptionEngines_clearRetained(SubscriptionEngines* se, char* topicName)
{
	FUNC_ENTRY;
	if (strncmp(topicName, sysprefix, strlen(sysprefix)) == 0)
		Log(LOG_AUDIT, 65, NULL, topicName);
	else
	{
		ListElement* current = NULL;
		ListNextElement(se->retaineds, &current);
		while (current)
		{
			RetainedPublications* r = current->content;
			ListNextElement(se->retaineds, &current);
			if (Topics_matches(topicName, r->topicName))
			{
				free(r->topicName);
				free(r->payload);
				ListRemove(se->retaineds, r);
				(se->retained_changes)++;
			}
		}
	}
	FUNC_EXIT;
}


#if defined(SUBSENGINE_UNIT_TESTS)

#if !defined(ARRAY_SIZE)
/**
 * Macro to calculate the number of entries in an array
 */
#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))
#endif

void SubscriptionEngines_List(SubscriptionEngines* se)
{
	ListElement* elem = NULL;

	printf("Subscriptions:\n");
	while (ListNextElement(se->subs, &elem))
	{
		Subscriptions* s = (Subscriptions*)(elem->content);
		printf("Subscription: %s %s %d\n", s->clientName, s->topicName, s->qos);
	}
	printf("End subscriptions\n\n");
}

char *newTopic(char* str)
{
	char* n = malloc(strlen(str)+1);
	strcpy(n, str);
	return n;
}

int main(int argc, char *argv[])
{
	char* aClientid = "test_client";
	int i, *ip, *todelete;

	SubscriptionEngines* se = SubscriptionEngines_initialize();

	SubscriptionEngines_subscribe(se, aClientid, newTopic("aaa"), 0, 0, 0);
	SubscriptionEngines_subscribe(se, aClientid, newTopic("#"), 1, 0, 0);
	SubscriptionEngines_List(se);

	SubscriptionEngines_subscribe(se, aClientid, newTopic("aaa"), 2, 0, 0);
	SubscriptionEngines_List(se);

	printf("Unsubscribing all\n");
	SubscriptionEngines_unsubscribe(se, aClientid, "#");
	SubscriptionEngines_List(se);

	SubscriptionEngines_terminate(se);

  #define _strdup strdup

	/* most specific tests */
	struct
	{
		char* topic1;
		char* topic2;
		char* result;
	} tests3[] = {
		{"+/C", "Topic/+", "Topic/+"},
		{"a/#", "b/+", "b/+"},
		{"aa/+", "bbb/#", "bbb/#"},
		{"aa/+", "bb/+", "bb/+"},
	};

	for (i = 0; i < ARRAY_SIZE(tests3); ++i)
	{
		printf("topic1: %s, topic2: %s, most_specific %s\n", tests3[i].topic1, tests3[i].topic2,
			SubscriptionEngines_mostSpecific(_strdup(tests3[i].topic1), _strdup(tests3[i].topic2)));
		assert(strcmp(SubscriptionEngines_mostSpecific(_strdup(tests3[i].topic1), _strdup(tests3[i].topic2)), tests3[i].result) == 0);
	}
}

void Log(int log_level, int msgno, char* format, ...)
{

}

#endif


