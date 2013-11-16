/*******************************************************************************
 * Copyright (c) 2008, 2013 IBM Corp.
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

#if !defined(USERS_H)
#define USERS_H

#include "LinkedList.h"

#define ACL_FULL 0
#define ACL_WRITE 1
#define ACL_READ 2

/*BE
include "LinkedList"
BE*/
/*BE
map permission
{
   "full" .
   "write" .
   "read"  .
}
def RULE
{
   n32 ptr STRING open "topic"
   n32 map permission "permission"
}
defList(RULE)
BE*/

/*BE
def USER
{
   n32 ptr STRING open "username"
   n32 ptr STRING open "password"
   n32 ptr RULEList open "acl"
}
defList(USER)
BE*/
typedef struct
{
	char* username;           /**< username */
	char* password;           /**< password */
	List* acl;                /**< Access Control List */
} User;

typedef struct
{
	char* topic;
	int permission;
} Rule;



void Users_add_user(char* username, char* pword);
void Users_free_list();
int Users_authenticate(char* username, char* pword);
User* Users_get_user(char* username);

void Users_add_default_rule(char* topic, int permission);
void Users_add_rule(User* user, char* topic, int permission);

int Users_authorise(User* user, char* topic, int action);

#endif /* USERS_H_ */
