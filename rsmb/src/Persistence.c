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
 * Persistence functions - reading information from and saving data to disk
 */

#include "Persistence.h"
#include "Log.h"
#include "Broker.h"
#include "Socket.h"
#include "SubsEngine.h"
#include "Bridge.h"
#include "MQTTProtocol.h"
#include "Topics.h"
#include "Messages.h"
#include "Clients.h"
#include "Users.h"
#include "StackTrace.h"

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <memory.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "Heap.h"

#if defined(WIN32)
#define snprintf _snprintf
#endif

int (*functions[])(char*);

enum {
	PROPERTY_INT, PROPERTY_STRING, PROPERTY_BOOLEAN, PROPERTY_STRING_LIST, PROPERTY_FUNCTION
} PROPERTY_TYPE;

/**
 * Property values to be read from the configuration file
 */
typedef struct
{
	char* name;		/**< name of the property string */
	int type;		/**< 0 = int, 1 = string, 2 = boolean, 3 = list of strings, 4 = function */
	int location;	/**< pointer to the position in the structure where this value is stored */
} property;



/**
 * Broker properties
 */
property brokerProps[] =
{
	{ "log_level", PROPERTY_INT, offsetof(trace_settings_type, log_level) },
	{ "max_log_entries", 0, offsetof(trace_settings_type, max_log_entries) },
	{ "trace_level", 0, offsetof(trace_settings_type, trace_level) },
	{ "trace_output", 4, 1 },
	{ "max_trace_entries", 0, offsetof(trace_settings_type, max_trace_entries) },
	{ "connection_messages", 2, offsetof(BrokerStates, connection_messages) },
	{ "max_inflight_messages", 0, offsetof(BrokerStates, max_inflight_messages) },
	{ "max_queued_messages", 0, offsetof(BrokerStates, max_queued_messages) },
	{ "retry_interval", 0, offsetof(BrokerStates, retry_interval) } ,
	{ "ffdc_output", 1, offsetof(BrokerStates, ffdc_location) },
	{ "persistence_location", 1, offsetof(BrokerStates, persistence_location) },
	{ "retained_persistence", 2, offsetof(BrokerStates, persistence) },
	{ "persistence", 2, offsetof(BrokerStates, persistence) }, /* synonym for retained_persistence */
	{ "autosave_on_changes", 2, offsetof(BrokerStates, autosave_on_changes) },
	{ "autosave_interval", PROPERTY_INT, offsetof(BrokerStates, autosave_interval) },
	{ "clientid_prefixes", 3, offsetof(BrokerStates, clientid_prefixes) },
#if !defined(NO_BRIDGE)
	{ "connection", 1, offsetof(BridgeConnections, name) },
#endif
#if defined(SINGLE_LISTENER)
	{ "port", 0, offsetof(BrokerStates, port) },
	{ "bind_address", 1, offsetof(BrokerStates, bind_address) },
	{ "max_connections", 0, offsetof(BrokerStates, max_connections) },
#else
	{ "listener", 1, 0 },
	{ "port", 0, 0 },
	{ "bind_address", 1, 0 },
	{ "max_connections", 0, 0 },
	{ "ipv6", 2, 0 },
#endif
	{ "password_file", 1, offsetof(BrokerStates, password_file) },
	{ "acl_file", 1, offsetof(BrokerStates, acl_file) },
	{ "allow_anonymous", 2, offsetof(BrokerStates, allow_anonymous) },
#if defined(MQTTS)
	{ "max_mqtts_packet_size", PROPERTY_INT, offsetof(BrokerStates, max_mqtts_packet_size) },
#endif
};

#if !defined(ARRAY_SIZE)
/**
 * Macro to calculate the number of entries in an array
 */
#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))
#endif

/**
 * size of the broker property array
 */
#define broker_props_count ARRAY_SIZE(brokerProps)

#if !defined(NO_BRIDGE)
/**
 * Bridge connection properties
 */
property bridgeProps[] =
{
	{ "notifications", 2, offsetof(BridgeConnections, notifications) },
	{ "round_robin", 2, offsetof(BridgeConnections, round_robin) },
	{ "try_private", 2, offsetof(BridgeConnections, try_private) },
	{ "address", 3, offsetof(BridgeConnections, addresses) }, /* synonym for addresses */
	{ "addresses", 3, offsetof(BridgeConnections, addresses) },
	{ "topic", 1, offsetof(BridgeConnections, topics) },
	{ "connection", 1, offsetof(BridgeConnections, name) },
	{ "notification_topic", 1, offsetof(BridgeConnections, notification_topic) },
	{ "keepalive_interval", 0, offsetof(BridgeConnections, keepalive_interval) },
	{ "start_type", 0, offsetof(BridgeConnections, start_type) },
	{ "idle_timeout", 0, offsetof(BridgeConnections, idle_timeout) },
	{ "threshold", 0, offsetof(BridgeConnections, threshold) },
	{ "cleansession", 2, offsetof(BridgeConnections, cleansession) },
#if defined(MQTTS)
	{ "protocol", 1, offsetof(BridgeConnections, protocol) },
	{ "loopback", PROPERTY_INT, offsetof(BridgeConnections, loopback) },
#endif
#if !defined(SINGLE_LISTENER)
	{ "listener", 1, 0 },
#endif
	{ "username", 1, offsetof(BridgeConnections, username) },
	{ "password", 1, offsetof(BridgeConnections, password) },
	{ "clientid", 1, offsetof(BridgeConnections, clientid) },
};
/**
 * size of the bridge properties array
 */
#define bridge_props_count ARRAY_SIZE(bridgeProps)
#endif

#if !defined(SINGLE_LISTENER)
/**
 * Listener properties
 */
property listenerProps[] =
{
	{ "listener", 1, 0 },
	{ "max_connections", 0, offsetof(Listener, max_connections) },
	{ "mount_point", 1, offsetof(Listener, mount_point) },
	{ "ipv6", 2, offsetof(Listener, ipv6) },
#if defined(MQTTS)
	{ "multicast_groups", 3, offsetof(Listener, multicast_groups) },
	{ "advertise", 1, offsetof(Listener, advertise) },
	{ "loopback", PROPERTY_INT, offsetof(Listener, loopback) },
#endif
	{ "connection", 1, offsetof(BridgeConnections, name) },
};
/**
 * size of the listener properties array
 */
#define listener_props_count ARRAY_SIZE(listenerProps)
#endif

#if defined(WIN32)
/**
 * strtok_r mapping for Windows
 */
#define strtok_r strtok_s
#else
/**
 * _unlink mapping for linux
 */
#define _unlink unlink
#endif

char* delims = " \t\r\n";	/**< token delimiters string for strtok */
char* pwordDelims = ":\r\n";

static BrokerStates* bstate;	/**< pointer to broker state structure */

int Persistence_process_acl_file(FILE* afile, BrokerStates* bs)
{
	User* currentUser = NULL;
	char curline[120], *curpos, *topic;
	int rc = 0;
	int line = 0;

	FUNC_ENTRY;
	while (fgets(curline, sizeof(curline),afile))
	{
		char* command = strtok_r(curline,delims, &curpos);
		if (command && command[0] != '\0' && command[0] != '#')
		{
			if (strcmp(command, "user") == 0)
			{
				char* user = strtok_r(NULL, delims, &curpos);
				if ((currentUser = Users_get_user(user)) == NULL)
				{
					rc = -98;
					Log(LOG_WARNING, 40, NULL, user, line);
					break;
				}
			}
			else if (strcmp(command, "topic") == 0)
			{
				// scan to the first non-whitespace char
				while(curpos[0] != '\0' && (curpos[0]==' ' || curpos[0]=='\t' || curpos[0]=='\n'|| curpos[0]=='\r'))
					curpos++;
				if (strlen(curpos) > 0)
				{
					// there is meaningful content on this line
					int permission = ACL_FULL;

					// check for the two permissions (read/write) followed by whitespace
					if (strncmp(curpos,"read",4)==0 && strlen(curpos)>4 && (curpos[4]==' ' || curpos[4]=='\t'))
					{
						permission = ACL_READ;
						curpos += 4;
					}
					else if (strncmp(curpos,"write",5)==0 && strlen(curpos)>5 && (curpos[5]==' ' || curpos[5]=='\t'))
					{
						permission = ACL_WRITE;
						curpos += 5;
					}
					// if a permission token was read, ignore the next block of whitespace
					// to reach the start of the topic
					if (permission != ACL_FULL)
						while(curpos[0] != '\0' && (curpos[0]==' ' || curpos[0]=='\t'))
							curpos++;

					// strip off any end-of-line chars
					topic = strtok(curpos, "\r\n");

					if (permission == ACL_FULL || permission == ACL_READ)
					{
						if ( (strchr(topic, '+') != NULL) || (strcspn(topic,"#") < strlen(topic) - 1))
						{
							rc = -98;
							Log(LOG_WARNING, 151, NULL, topic);
							break;
						}
					}


					if (currentUser == NULL)
						Users_add_default_rule(topic,permission);
					else
						Users_add_rule(currentUser,topic,permission);
				}
				else
				{
					rc = -98;
					Log(LOG_WARNING, 41, NULL, line);
					break;
				}
			}
			else
			{
				rc = -98;
				Log(LOG_WARNING, 41, NULL, line);
				break;
			}
		}
		line++;
	}
	FUNC_EXIT_RC(rc);
	return rc;
}

int Persistence_process_user_file(FILE* ufile, BrokerStates* bs)
{
	char curline[120], *curpos;
	int rc = 0;
	int line = 1;

	FUNC_ENTRY;
	while (fgets(curline, sizeof(curline),ufile))
	{
		char* user = strtok_r(curline, pwordDelims, &curpos);
		if (user && user[0] != '\0' && user[0] != '#')
		{
			char* password = strtok_r(NULL, pwordDelims, &curpos);
			if (password)
				Users_add_user(user, password);
			else
			{
				Log(LOG_WARNING, 39, NULL, line);
				rc = -98;
				break;
			}
		}
		line++;
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Return a string token, allowing the inclusion of spaces by enclosing with ", and the inclusion of "
 * by doubling "
 * @param str char string as per strtok_r
 * @param delimiter string as per strtok_r
 * @param curpos char pointer as per strtok_r
 * @return pointer to the token string
 */
char* getstring(char* str, char* delims, char** curpos)
{
	char* rc = NULL;
	static char buf[128];

	buf[0] = '\0';
	while (**curpos == ' ') /* skip leading whitespace */
		(*curpos)++;
	if (**curpos == '\"') /* if the first character of the token is ", it must finish with " or the end of the line */
	{
		if (*((*curpos)+1) == '\"') /* if the next character is also ", then we have a 0 length string */
			(*curpos) += 2; /* skip the two "" */
		else
		{
			strcat(buf, strtok_r(str, "\"", curpos));
			while (**curpos == '\"')
			{
				strcat(buf, "\"");
				strcat(buf, strtok_r(str, "\"", curpos));
			}
		}
		rc = buf;
	}
	else
		rc = strtok_r(str, delims, curpos);
	return rc;
}


/**
 * Process a configuration file
 * @param cfile configuration file handle
 * @param bs pointer to the broker state structure
 * @param propsTable property table to use
 * @param props_count size of the property table
 * @return pointer to the default listener, if any
 */
#if !defined(SINGLE_LISTENER)
	Listener*
#else
	void
#endif
Persistence_process_file(FILE* cfile, BrokerStates* bs, property* propsTable, int props_count)
{
	void* s = bs;
	char curline[120], *curpos;
	int line_no = 0;
#if !defined(SINGLE_LISTENER)
	Listener* defaultListener = NULL;
#endif

	FUNC_ENTRY;
	while (fgets(curline, sizeof(curline), cfile))
	{
		char *pword = strtok_r(curline, delims, &curpos);

		line_no++;
		if (pword && pword[0] != '\0' && pword[0] != '#')
		{
			int i = 0;
			int found = 0;
			for (i = 0; i < props_count; ++i)
			{
				if (strcmp(pword, propsTable[i].name) == 0)
				{
					char* val = getstring(NULL, delims, &curpos);
					found = 1;
					if (!val && !(propsTable[i].type == 4 && strcmp(pword, "stop") == 0))
						Log(LOG_WARNING, 1, NULL, pword, line_no);
					else
					{
						if (strcmp(pword, "log_level") == 0)
						{
							if (strcmp(val, "config") == 0)
								trace_settings.log_level = LOG_CONFIG;
							else if (strcmp(val, "detail") == 0)
								trace_settings.log_level = LOG_DETAIL;
							else if (strcmp(val, "info") == 0)
								trace_settings.log_level = LOG_INFO;
							else if (strcmp(val, "audit") == 0)
								trace_settings.log_level = LOG_AUDIT;
							else if (strcmp(val, "warning") == 0)
								trace_settings.log_level = LOG_WARNING;
							else if (strcmp(val, "error") == 0)
								trace_settings.log_level = LOG_ERROR;
							else
								Log(LOG_WARNING, 152, NULL, val, line_no);
						}
						else if (strcmp(pword, "trace_level") == 0)
						{
							if (strcmp(val, "minimum") == 0)
								trace_settings.trace_level = TRACE_MINIMUM;
							else if (strcmp(val, "medium") == 0)
								trace_settings.trace_level = TRACE_MEDIUM;
							else if (strcmp(val, "maximum") == 0)
								trace_settings.trace_level = TRACE_MAXIMUM;
							else
								Log(LOG_WARNING, 152, NULL, val, line_no);
						}
						else if (strcmp(pword, "max_log_entries") == 0)
							trace_settings.max_log_entries = atoi(val);
						else if (strcmp(pword, "max_trace_entries") == 0)
							trace_settings.max_trace_entries = atoi(val);
						else
#if !defined(SINGLE_LISTENER)
						if (propsTable != listenerProps && strcmp(pword, "bind_address") == 0)
						{
							if (defaultListener == NULL)
								defaultListener = Socket_new_listener();
							defaultListener->address = malloc(strlen(val)+1);
							strcpy(defaultListener->address, val);
						}
						else if (propsTable != listenerProps && strcmp(pword, "port") == 0)
						{
							if (defaultListener == NULL)
								defaultListener = Socket_new_listener();
							defaultListener->port = atoi(val);
						}
						else if (propsTable != listenerProps && strcmp(pword, "max_connections") == 0)
						{
							if (defaultListener == NULL)
								defaultListener = Socket_new_listener();
							defaultListener->max_connections = atoi(val);
						}
						else if (propsTable != listenerProps && strcmp(pword, "ipv6") == 0)
						{
							if (defaultListener == NULL)
								defaultListener = Socket_new_listener();
							defaultListener->ipv6 = (strcmp(val, "true") == 0);
						}
						else if (strcmp(pword, "listener") == 0)
						{
							/* listener port [address] [mqtt|mqtts] */
							propsTable = listenerProps;
							props_count = listener_props_count;
							s = Socket_new_listener();
							((Listener*)s)->port = atoi(val);
							if ((val = strtok_r(NULL, delims, &curpos)))
							{
								((Listener*)s)->address = malloc(strlen(val)+1);
								strcpy(((Listener*)s)->address, val);
								if ((val = strtok_r(NULL, delims, &curpos)))
								{
									if (strcmp(val, "mqtt") == 0)
										((Listener*)s)->protocol = PROTOCOL_MQTT;
#if defined(MQTTS)
									else if (strcmp(val, "mqtts") == 0)
										((Listener*)s)->protocol = PROTOCOL_MQTTS;
#endif
									else
										Log(LOG_WARNING, 152, NULL, val, line_no);
								}
							}
							ListAppend(bs->listeners, s, 0);
						}
						else
#endif
#if !defined(NO_BRIDGE)
						if (strcmp(pword, "connection") == 0)
						{
							propsTable = bridgeProps;
							props_count = bridge_props_count;
							s = Bridge_new_connection(&(bs->bridge), val);
							if (s == NULL)
								goto exit;
							if (bs->bridge.connections == NULL)
								bs->bridge.connections = ListInitialize();
							ListAppend(bs->bridge.connections, s, 0);
							Log(LOG_CONFIG, 6, NULL, val, "connections");
						}
						else if (strcmp(pword, "start_type") == 0)
						{
							if (strcmp(val, "automatic") == 0)
								((BridgeConnections*)s)->start_type = START_AUTOMATIC;
							else if (strcmp(val, "manual") == 0)
								((BridgeConnections*)s)->start_type = START_MANUAL;
							else if (strcmp(val, "lazy") == 0)
								((BridgeConnections*)s)->start_type = START_LAZY;
							else if (strcmp(val, "once") == 0)
								((BridgeConnections*)s)->start_type = START_ONCE;
							else
								Log(LOG_WARNING, 152, NULL, val, line_no);
						}
						else if (strcmp(pword, "topic") == 0)
						{
							/* List of topic structures: pattern, direction, localPrefix, remotePrefix, priority */
							int ok = 0;
							BridgeTopics* t = malloc(sizeof(BridgeTopics));
#if defined(MQTTS)
							t->subscribed = 0;
#endif
							t->localPrefix = t->remotePrefix = NULL;
							t->pattern = malloc(strlen(val)+1);
							strcpy(t->pattern, val);
							ok = 1;
							t->direction = 2; /* default - out */
							t->priority = PRIORITY_NORMAL;
							if ((val = getstring(NULL, delims, &curpos)))
							{
								if (strcmp(val, "both") == 0)
									t->direction = 0;
								else if (strcmp(val, "in") == 0)
									t->direction = 1;
								else if (strcmp(val, "out") == 0)
									t->direction = 2;
								else
									Log(LOG_WARNING, 2, NULL, val);
								if ((val = getstring(NULL, delims, &curpos)))
								{
									if (val[0] != '\0')
									{
										t->localPrefix = malloc(strlen(val)+1);
										strcpy(t->localPrefix, val);
									}
									if ((val = getstring(NULL, delims, &curpos)))
									{
										if (val[0] != '\0')
										{
											t->remotePrefix = malloc(strlen(val)+1);
											strcpy(t->remotePrefix, val);
										}
										if ((val = getstring(NULL, delims, &curpos)))
										{
											if (val[0] != '\0')
											{
												if (strcmp(val, "high") == 0)
													t->priority = PRIORITY_HIGH;
												else if (strcmp(val, "low") == 0)
													t->priority = PRIORITY_LOW;
											}
										}
									}
								}
							}
							if (ok)
								ListAppend(((BridgeConnections*)s)->topics, t, sizeof(BridgeTopics));
						}
						else
#if defined(MQTTS)
						if (strcmp(pword, "protocol") == 0)
						{
							if (strcmp(val, "mqtt") == 0)
								((BridgeConnections*)s)->protocol = PROTOCOL_MQTT;
							else if (strcmp(val, "mqtts") == 0)
								((BridgeConnections*)s)->protocol = PROTOCOL_MQTTS;
							else if (strcmp(val, "mqtts_multicast") == 0)
								((BridgeConnections*)s)->protocol = PROTOCOL_MQTTS_MULTICAST;
						}
						else if (strcmp(pword, "advertise") == 0)
						{
							/* advertise address interval gateway_id */
							advertise_parms* adv = malloc(sizeof(advertise_parms));
							memset(adv, '\0', sizeof(advertise_parms));
							adv->address = malloc(strlen(val) + 1);
							strcpy(adv->address, val);
							if ((val = strtok_r(NULL, delims, &curpos)))
							{
								adv->interval = atoi(val);
								if ((val = strtok_r(NULL, delims, &curpos)))
								  adv->gateway_id = atoi(val);
							}
							((Listener*)s)->advertise = adv;
						}
						else
#endif
#endif
						if (propsTable[i].type == PROPERTY_STRING)
						{
							char** loc = (char**)&((char*)s)[propsTable[i].location];
							*loc = malloc(strlen(val)+1);
							strcpy(*loc, val);
							Log(LOG_CONFIG, 5, NULL, pword, val);
						}
						else if (propsTable[i].type == PROPERTY_BOOLEAN)
						{ /* boolean */
							int ival = -1;
							if (strcmp(val, "true") == 0)
								ival = 1;
							else if (strcmp(val, "false") == 0)
								ival = 0;
							else
								Log(LOG_WARNING, 4,  NULL, val, line_no);
							if (ival != -1)
							{
								Log(LOG_CONFIG, 5, NULL, pword, val);
								memcpy(&(((char*)s)[propsTable[i].location]), &ival, sizeof(int));
							}
						}
						else if (propsTable[i].type == PROPERTY_STRING_LIST)
						{
							List** list = (List**)&((char*)s)[propsTable[i].location];
							/* list of strings */
							while (val)
							{
								int len = strlen(val)+1;
								char *loc = malloc(len);
								strcpy(loc, val);
								ListAppend(*list, loc, len);
								Log(LOG_CONFIG, 6, NULL, val, pword);
								//val = strtok_r(NULL, delims, &curpos);
								val = getstring(NULL, delims, &curpos);
							}
						}
#if !defined(NO_ADMIN_COMMANDS)
						else if (propsTable[i].type == PROPERTY_FUNCTION)
						{
							/* call the function pointed to in the table */
							(*functions[propsTable[i].location])(val);
						}
#endif
						else
						{
							int ival = atoi(val);
							Log(LOG_CONFIG, 7, NULL, pword, ival);
							memcpy(&(((char*)s)[propsTable[i].location]), &ival, sizeof(int));
						}
					}
					break;
				} /* if */
			} /* for */
			if (!found)
				Log(LOG_WARNING, 8, NULL, pword, line_no);
		} /* if */
	} /* while */

#if !defined(NO_BRIDGE)
	exit:;
#endif
	FUNC_EXIT;
#if !defined(SINGLE_LISTENER)
	return defaultListener;
#endif
}


/**
 * Read the broker configuration file
 * @param filename name of the configuration file
 * @param bs pointer to the broker state structure
 * @param config_set has a config file been named at startup?
 * @return success indicator - success == 1
 */
int Persistence_read_config(char* filename, BrokerStates* bs, int config_set)
{
	int rc = 0;
	FILE* cfile = NULL;
#if !defined(SINGLE_LISTENER)
	Listener* defaultListener = NULL;
#endif
#if !defined(NO_BRIDGE)
	ListElement* bridgeConnectionElem = NULL;
#endif

	FUNC_ENTRY;
	bs->clientid_prefixes = ListInitialize();
	bstate = bs;
	if ((cfile = fopen(filename, "r")) == NULL)
	{
		if (config_set)
		{
			Log(LOG_WARNING, 0, NULL, filename);
			rc = -98;
		}
	}
	else
	{
#if !defined(SINGLE_LISTENER)
		defaultListener =
#endif
		Persistence_process_file(cfile, bs, brokerProps, broker_props_count);
		fclose(cfile);
#if !defined(SINGLE_LISTENER)
		if (defaultListener != NULL)
			ListAppend(bs->listeners, defaultListener, 0);
#endif
	}
#if !defined(SINGLE_LISTENER)
	if (bs->listeners->count == 0)
	{
		defaultListener = Socket_new_listener();
		ListAppend(bs->listeners, defaultListener, 0);
	}
#endif

	bs->users = ListInitialize();

#if !defined(NO_BRIDGE)
	if (bs->bridge.connections != NULL)
	{
		while (ListNextElement(bs->bridge.connections, &bridgeConnectionElem))
			if (((BridgeConnections*)(bridgeConnectionElem->content))->addresses->count == 0)
			{
				rc = -97;
				Log(LOG_WARNING, 142, NULL, ((BridgeConnections*)(bridgeConnectionElem->content))->name);
				goto exit;
			}
	}
#endif

	if (bs->password_file)
	{
		FILE* ufile = NULL;
		if ((ufile = fopen(bs->password_file,"r")) == NULL)
		{
			Log(LOG_WARNING, 0, NULL, bs->password_file);
			rc = -98;
		}
		else
		{
			rc = Persistence_process_user_file(ufile,bs);
			fclose(ufile);
		}

		if ((rc==0) && bs->acl_file)
		{
			FILE* afile = NULL;
			if ((afile = fopen(bs->acl_file,"r")) == NULL)
			{
				Log(LOG_WARNING, 0, NULL, bs->acl_file);
				rc = -98;
			}
			else
			{
				rc = Persistence_process_acl_file(afile,bs);
				fclose(afile);
			}
		}

	}
	else if (bs->acl_file)
	{
		Log(LOG_WARNING, 25, NULL);
		rc = -98;
	}

#if !defined(NO_BRIDGE)
	exit:
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free configuration storage
 * @param bs pointer to a broker state structure
 */
void Persistence_free_config(BrokerStates* bs)
{
#if !defined(SINGLE_LISTENER)
	ListElement* current = NULL;
#endif

	FUNC_ENTRY;
	if (bs->persistence_location)
		free(bs->persistence_location);
	if (bs->ffdc_location)
		free(bs->ffdc_location);
	ListFree(bs->clientid_prefixes);

    if (bs->password_file)
    	free(bs->password_file);
    if (bs->acl_file)
    	free(bs->acl_file);

    Users_free_list();

#if !defined(SINGLE_LISTENER)
	while (ListNextElement(bs->listeners, &current))
	{
		Listener* list = (Listener*)(current->content);
		ListFree(list->connections);
		if (list->address)
			free(list->address);
		if (list->mount_point)
			free(list->mount_point);
#if defined(MQTTS)
		ListFree(list->multicast_groups);
		if (list->advertise)
		{
			free(list->advertise->address);
			free(list->advertise);
		}
#endif
	}
	ListFree(bs->listeners);
#else
	if (bs->bind_address)
		free(bs->bind_address);
#endif
	FUNC_EXIT;
}


static FILE* rfile = NULL;	/**< Current persistence file handle */
static char *cur_fn, *cur_backup_fn, *cur_backup_fn1;


/**
 * Add the persistence location prefix to a filename
 * @param fn the name of the file to which to add the prefix
 * @return the prefix+filename
 */
char* add_prefix(char* fn)
{
	char* rc = fn;

	FUNC_ENTRY;
	if (bstate->persistence_location)
	{
		rc = malloc(strlen(fn) + strlen(bstate->persistence_location)+1);
		strcpy(rc, bstate->persistence_location);
		strcat(rc, fn);
	}
	FUNC_EXIT;
	return rc;
}


/**
 * Free prefix storage, if it was set
 * @param rc pointer to the (maybe) changed string
 * @param fn pointer to the original filename string
 */
void free_prefix(char* rc, char* fn)
{
	FUNC_ENTRY;
	if (rc != fn)
		free(rc);
	FUNC_EXIT;
}


/**
 * Open a persisted data file
 * @param mode file mode to use
 * @param fn file name
 * @param backup_fn backup file name
 * @param backup_fn1 secondary backup file name
 * @return pointer to the file handle
 */
FILE* Persistence_open_common(char mode, char* fn, char* backup_fn, char* backup_fn1)
{
	char *type = (fn[7] == 'r') ? Messages_get(139, LOG_INFO) : Messages_get(140, LOG_INFO);

	FUNC_ENTRY;
	cur_fn = fn;
	cur_backup_fn = backup_fn;
	cur_backup_fn1 = backup_fn1;
	if (!bstate->persistence)
		rfile = NULL;
	else if (mode == 'w')
	{
		char* loc = add_prefix(fn);
		char* bak = add_prefix(backup_fn);
		char* bak1 = add_prefix(backup_fn1);

		_unlink(bak1);         /* make room for second backup */
		rename(bak, bak1);     /* move backup to second backup */
		rename(loc, bak);      /* move current state file to backup */

		/* open new file */
		if ((rfile = fopen(loc, "wb")) == NULL)
			Log(LOG_WARNING, 9, NULL, type, loc, type);
		free_prefix(loc, fn);
		free_prefix(bak, backup_fn);
		free_prefix(bak1, backup_fn1);
	}
	else if (mode == 'r')
	{
		char* loc = add_prefix(fn);
		if ((rfile = fopen(loc, "rb")) == NULL)
			Log(LOG_WARNING, 10, NULL, type, loc, type);
		else
			Log(LOG_INFO, 11, NULL, type, loc);
		free_prefix(loc, fn);
	}
	FUNC_EXIT;
	return rfile;
}


/**
 * Open the retained message persistence file
 * @param mode file mode to use
 * @return the opened file handle
 */
FILE* Persistence_open_retained(char mode)
{
	return Persistence_open_common(mode, "broker.rms", "broker.1ms", "broker.2ms");
}


/**
 * Open the subscription persistence file
 * @param mode file mode to use
 * @return the opened file handle
 */
FILE* Persistence_open_subscriptions(char mode)
{
	return Persistence_open_common(mode, "broker.sub", "broker.1ub", "broker.2ub");
}


/**
 * Write a retained message to the current persistence file
 * @param payload the message contents
 * @param payloadlen the length of the payload
 * @param qos quality of service
 * @param topicName the name of the topic
 * @return success indicator - success = 0, -1 otherwise
 */
int Persistence_write_retained(char* payload, int payloadlen, int qos, char* topicName)
{
	int rc = 0;

	FUNC_ENTRY;
	if (rfile)
	{
		int topiclen = strlen(topicName);
		if (fwrite(&payloadlen, sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(payload, payloadlen, 1, rfile) != 1)
			rc = -1;
		if (fwrite(&qos, sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(&topiclen, sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(topicName, topiclen, 1, rfile) != 1)
			rc = -1;
	}
	else
		rc = -1;
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Read a retained publication from the current persistence file
 * @return the retained publication read from the file
 */
RetainedPublications* Persistence_read_retained()
{
	RetainedPublications* r = NULL;

	FUNC_ENTRY;
	if (rfile != NULL)
	{
		int topiclen;
		int success = 0;
		r = malloc(sizeof(RetainedPublications));

		if (fread(&(r->payloadlen), sizeof(int), 1, rfile) == 1)
		{
			r->payload = malloc(r->payloadlen);
			if (fread(r->payload, r->payloadlen, 1, rfile) == 1 &&
				fread(&(r->qos), sizeof(int), 1, rfile) == 1 &&
				fread(&topiclen, sizeof(int), 1, rfile) == 1)
			{
				r->topicName = malloc(topiclen + 1);
				if (fread(r->topicName, topiclen, 1, rfile) == 1)
				{
					r->topicName[topiclen] = '\0';
					success = 1;
				}
				else
				{
					free(r->topicName);
					free(r->payload);
				}
			}
			else
				free(r->payload);
		}
		if (!success)
		{
			free(r);
			r = NULL;
		}
	}
	FUNC_EXIT;
	return r;
}


/**
 * Write a subscription to the current persistence file
 * @param s pointer to the subcription information
 * @return success indicator - success = 0, -1 otherwise
 */
int Persistence_write_subscription(Subscriptions* s)
{
	int rc = 0;

	FUNC_ENTRY;
	if (rfile)
	{
		int len = strlen(s->clientName);

		if (fwrite(&len, sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(s->clientName, len, 1, rfile) != 1)
			rc = -1;
		if (fwrite(&(s->noLocal), sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(&(s->qos), sizeof(int), 1, rfile) != 1)
			rc = -1;
		len = strlen(s->topicName);
		if (fwrite(&len, sizeof(int), 1, rfile) != 1)
			rc = -1;
		if (fwrite(s->topicName, len, 1, rfile) != 1)
			rc = -1;
	}
	else
		rc = -1;
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Create a default client structure with suitably allocated storage
 * @param clientID the client id string to use
 * @return pointer to the allocated client structure
 */
Clients* Persistence_createDefaultClient(char* clientID)
{
	Clients* newc = malloc(sizeof(Clients));
	int i;

	FUNC_ENTRY;
	memset(newc, '\0', sizeof(Clients));
	newc->clientID = clientID;
	newc->outboundMsgs = ListInitialize();
	newc->inboundMsgs = ListInitialize();
	for (i = 0; i < PRIORITY_MAX; ++i)
		newc->queuedMsgs[i] = ListInitialize();
	FUNC_EXIT;
	return newc;
}


/**
 * Read a subscription entry from the current persistence file
 * @return pointer to the subscription structure read, or NULL if there was an error
 */
Subscriptions* Persistence_read_subscription()
{
	Subscriptions* s = NULL;

	FUNC_ENTRY;
	if (rfile != NULL)
	{
		int len;
		int success = 0;
		s = malloc(sizeof(Subscriptions));
		memset(s, '\0', sizeof(Subscriptions));

		if (fread(&(len), sizeof(int), 1, rfile) == 1)
		{
			/* s->was_persisted = 1; */
			s->clientName = malloc(len+1);
			s->clientName[len] = '\0';

			if (fread(s->clientName, len, 1, rfile) == 1 &&
				fread(&(s->noLocal), sizeof(int), 1, rfile) == 1 &&
				fread(&(s->qos), sizeof(int), 1, rfile) == 1 &&
				fread(&len, sizeof(int), 1, rfile) == 1)
			{
				s->topicName = malloc(len + 1);
				if (fread(s->topicName, len, 1, rfile) == 1)
				{
					Node* elem = NULL;
					s->topicName[len] = '\0';
					s->durable = 1;
					s->wildcards = Topics_hasWildcards(s->topicName);
					s->priority = PRIORITY_NORMAL;
					success = 1;
					 
					if ((elem = TreeFind(bstate->disconnected_clients, s->clientName)) == NULL)
					{
						/*printf("adding sub for client %s\n", s->clientName);*/ 
						TreeAdd(bstate->disconnected_clients, Persistence_createDefaultClient(s->clientName),
							sizeof(Clients) + strlen(s->clientName)+1 + 3*sizeof(List));
					}
					else
					{
						free(s->clientName);
						s->clientName = ((Clients*)(elem->content))->clientID;
					}
				}
				else
				{
					free(s->topicName);
					free(s->clientName);
				}
			}
			else
				free(s->clientName);
		}
		if (!success)
		{
			free(s);
			s = NULL;
		}
	}
	FUNC_EXIT;
	return s;
}


/**
 * Close the current persistence file.
 */
void Persistence_close_file(int write_error)
{
	if (rfile)
	{
		char* bak1 = add_prefix(cur_backup_fn1);
		
		int rc = fclose(rfile);
		
		if (write_error != 0 || rc != 0)
		{
			char* loc = add_prefix(cur_fn);
			char* bak = add_prefix(cur_backup_fn);

			_unlink(loc); /* remove the erroneously written persistence file */ 
			rename(bak, loc); /* restore the backup */
			rename(bak1, bak); /* restore the 2nd backup to backup */
		
			free_prefix(loc, cur_fn);
			free_prefix(bak, cur_backup_fn);
		}
		else
			_unlink(bak1);         /* remove the second backup */
			
		free_prefix(bak1, cur_backup_fn1);
	}
}


#if !defined(NO_ADMIN_COMMANDS)
int xxx_create_segfault(char* dest)
{
	int* ptr = (int*)0xFFFFFFFF;
	*ptr = 2;
	return 0;
}

int clear_retained(char* dest)
{
	SubscriptionEngines_clearRetained(bstate->se, dest);
	return 0;
}

int take_FFDC(char* symptoms)
{
	#define SYMPTOM_BUFFER_SIZE 256
	char symptom_buffer[SYMPTOM_BUFFER_SIZE];
	snprintf(symptom_buffer, SYMPTOM_BUFFER_SIZE, "Requested by command. %s", symptoms);
	symptom_buffer[SYMPTOM_BUFFER_SIZE - 1] = '\0'; 
	Broker_recordFFDC(symptom_buffer);
	return 0;
}

/**
 * function table of command implementations
 */
int (*functions[])(char*) =
{
	Broker_stop,
	Log_traceOutput,
	Log_dumpTrace,
	Broker_dumpHeap,
	take_FFDC,
	xxx_create_segfault,
	clear_retained,
#if !defined(NO_BRIDGE)
	Bridge_startConnection,
	Bridge_stopConnection,
	Bridge_deleteConnection,
#endif
};


/**
 * Property table for admin commands
 */
property commandProps[] =
{
	{ "stop", 4, 0 },
	{ "log_level", 0, offsetof(trace_settings_type, log_level) },
	{ "trace_level", 0, offsetof(trace_settings_type, trace_level) },
	{ "trace_output", 4, 1 },
	{ "trace_dump", 4, 2 },
	{ "heap_dump", 4, 3 },
	{ "take_ffdc", 4, 4 },
	{ "xxx_create_segfault", 4, 5 },
	{ "max_log_entries", 0, offsetof(trace_settings_type, max_log_entries) },
	{ "max_trace_entries", 0, offsetof(trace_settings_type, max_trace_entries) },
	{ "clear_retained", 4, 6 },
#if !defined(NO_BRIDGE)
	{ "start_connection", 4, 7 },
	{ "stop_connection", 4, 8 },
	{ "delete_connection", 4, 9 },
	{ "connection", 1, offsetof(BridgeConnections, name) },
#endif

};
/**
 * size of the admin commands property array
 */
#define command_props_count ARRAY_SIZE(commandProps)


/**
 * Read a command from the named update file and run it.
 * @param filename - the name of the file to read the admin command from
 * @param bs pointer to the broker state structure
 * @return boolean success indicator, success=1
 */
int Persistence_read_admin(char* filename, BrokerStates* bs)
{
	int success = 0;
	FILE* cfile = NULL;

	FUNC_ENTRY;
	if ((cfile = fopen(filename, "r")) != NULL)
	{
		Persistence_process_file(cfile, bs, commandProps, command_props_count);
		fclose(cfile);
		success = 1;
	}
	FUNC_EXIT_RC(success);
	return success;
}


/**
 * Read a command from the update file and run it.
 * @param bs pointer to the broker state structure
 */
void Persistence_read_command(BrokerStates* bs)
{
	char* fn = "broker.upd";
	char* cmd_file = add_prefix(fn);
	struct stat buf;

	FUNC_ENTRY;
	if (stat(cmd_file, &buf) != -1)
	{
		int saved = trace_settings.log_level;

		Log(LOG_INFO, 68, NULL, cmd_file);
		trace_settings.log_level = LOG_CONFIG;
		if (Persistence_read_admin(cmd_file, bs))
			_unlink(cmd_file);
		trace_settings.log_level = saved;
	}
	free_prefix(cmd_file, fn);
	FUNC_EXIT;
}

#endif
