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

/**
 * @file
 * User module.
 * Handles authentication and authorisation
 */

#include "LinkedList.h"
#include "Broker.h"
#include "SubsEngine.h"
#include "Topics.h"
#include "Log.h"
#include "StackTrace.h"

#include "Users.h"

#include <stdio.h>

#include "Heap.h"

BrokerStates* bstate;

void Users_initialize(BrokerStates* aBrokerState)
{
	FUNC_ENTRY;
	bstate = aBrokerState;
	bstate->defaultACL = ListInitialize();
	FUNC_EXIT;
}

/**
 * Adds the specified user to the known user list.
 * @param username
 * @param pword
 */
void Users_add_user(char* username, char* pword)
{
	User* u = malloc(sizeof(User));

	FUNC_ENTRY;
	memset(u, '\0', sizeof(User));
	u->username = malloc(strlen(username)+1);
	u->password = malloc(strlen(pword)+1);
	strcpy(u->username,username);
	strcpy(u->password,pword);
	u->acl = ListInitialize();
	ListAppend(bstate->users,u,sizeof(u)+strlen(username)+strlen(pword)+2);
	FUNC_EXIT;
}

/**
 * Frees the memory used by the provided list of ACL structs
 */
void Users_free_acl(List* list)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(list, &current))
	{
		Rule* rule = (Rule*)(current->content);
		free(rule->topic);
	}
	ListFree(list);
	FUNC_EXIT;
}

/**
 * Frees the memory used by the user list and all ACL lists
 */
void Users_free_list()
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(bstate->users, &current))
	{
		User* user = (User*)(current->content);
		free(user->username);
		free(user->password);
		Users_free_acl(user->acl);
	}
	ListFree(bstate->users);
	Users_free_acl(bstate->defaultACL);

	FUNC_EXIT;
}

int userCompare(void* a, void* b)
{
	User* user = (User*)a;
	return strcmp(user->username,(char*)b) == 0;
}

/**
 * Encrypts the provided password. Currently, this is a no-op as
 * passwords are stored in clear-text.
 * @param pword the unencrypted password
 * @return the encrypted password
 */
char* Users_encrypt_password(char* pword)
{
	// For now, handle things in clear-text
	return pword;
}

/**
 * Verifies the specified username/password is valid
 * @param username
 * @param pword
 * @return whether the user is authenticated - (true/false)
 */
int Users_authenticate(char* username, char* pword)
{
	User* user = NULL;
	int result = false;
	char* encryptedPword = Users_encrypt_password(pword);

	FUNC_ENTRY;
	if ((user = Users_get_user(username)) != NULL)
	{
		if (strcmp(user->password,encryptedPword)==0)
		{
			result = true;
		}
	}
	FUNC_EXIT_RC(result);
	return result;
}

/**
 * Gets the User struct for the specified username
 */
User* Users_get_user(char* username)
{
	User* user = NULL;
	ListElement* elem = NULL;

	if ((elem = ListFindItem(bstate->users, username, userCompare)) != NULL)
	{
		user = (User*)elem->content;
	}
	return user;
}

/**
 * Creates an ACL struct
 */
Rule* Users_create_rule(char* topic, int permission)
{
	Rule* rule = NULL;

	FUNC_ENTRY;
	rule = malloc(sizeof(Rule));
	rule->permission = permission;
	rule->topic = malloc(strlen(topic)+1);
	strcpy(rule->topic,topic);
	FUNC_EXIT;
	return rule;
}

/**
 * Adds the specified ACL information to the default ACL
 */
void Users_add_default_rule(char* topic, int permission)
{
	Rule* rule = NULL;

	FUNC_ENTRY;
	rule = Users_create_rule(topic,permission);
	ListAppend(bstate->defaultACL, rule, sizeof(rule)+strlen(topic)+1);
	FUNC_EXIT;
}

/**
 * Adds the specified ACL information to the specified User's ACL
 */
void Users_add_rule(User* user, char* topic, int permission)
{
	Rule* rule = NULL;

	FUNC_ENTRY;
	rule = Users_create_rule(topic,permission);
	ListAppend(user->acl, rule, sizeof(rule)+strlen(topic)+1);
	FUNC_EXIT;
}

int Users_subscription_matches(char* ruleTopic, char* subTopic)
{
	int rc = false;
	int subIsWC = Topics_hasWildcards(subTopic);

	FUNC_ENTRY;
	if (!subIsWC)
		rc = Topics_matches(ruleTopic, Topics_hasWildcards(ruleTopic), subTopic);
	else
	{
		int t1 = strcspn(ruleTopic, "#");
		rc = (strncmp(ruleTopic,subTopic,t1)==0);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}
/**
 * Finds the most specific Rule in the specified list for
 * the provided topic.
 */
int Users_authorise1(List* aclList, char* topic, int action)
{
	int rc = true;
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(aclList, &current))
	{
		Rule* rule = (Rule*)(current->content);
		if (action == ACL_WRITE)
		{
			if (Topics_matches(rule->topic, Topics_hasWildcards(rule->topic), topic))
				if (rule->permission == ACL_FULL || rule->permission == action)
					goto exit;
		}
		else
		{
			if (Users_subscription_matches(rule->topic, topic))
				if (rule->permission == ACL_FULL || rule->permission == action)
					goto exit;
		}
	}
	rc = false;
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Checks whether the specified user is allowed to access the specified topic
 */
int Users_authorise(User* user, char* topic, int action)
{
	int rc = true;

	FUNC_ENTRY;
	if (!Users_authorise1(bstate->defaultACL, topic, action))
	{
		if (user != NULL)
		{
			rc = Users_authorise1(user->acl, topic, action);
			goto exit;
		}
		else
		{
			rc = false;
			goto exit;
		}
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

