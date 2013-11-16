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


#if !defined(BRIDGE_H)
#define BRIDGE_H

#include "Clients.h"
#include "SubsEngine.h"
#include "MQTTPacket.h"

/*BE
include "Clients"
include "SubsEngine"
include "MQTTPacket"
include "MQTTProtocol"
BE*/

/*BE
map BRIDGE_TOPIC_DIRECTION
{
   "both" .
   "in" .
   "out" .
}

def BRIDGETOPICS
{
   n32 ptr STRING open "pattern"
   n32 ptr STRING open "localPrefix"
   n32 ptr STRING open "remotePrefix"
   n32 map BRIDGE_TOPIC_DIRECTION "direction"
$ifdef MQTTS
   n32 map bool "subscribed"
$endif
}
defList(BRIDGETOPICS)
BE*/
/**
 * Bridge topic structure
 */
typedef struct
{
	char *pattern,		/**< topic pattern */
		*localPrefix,	/**< local prefix from config */
		*remotePrefix;	/**< remote prefix from config */
	int direction;  	/**< out, in or both */
	int priority;
#if defined(MQTTS)
	int subscribed; 	/**< has this topic been subscribed to? */
#endif
} BridgeTopics;		/**< Bridge topic structure */

/*BE
map START_TYPES
{
	"START_AUTOMATIC" .
	"START_MANUAL" .
	"START_LAZY" .
	"START_ONCE" .
}
map RUN_STATE
{
	"CONNECTION_STOPPED" .
	"CONNECTION_RUNNING" .
	"CONNECTION_STOPPING" .
	"CONNECTION_STOPPING_THEN_DELETE" .
	"CONNECTION_DELETE" .
	"CONNECTION_SWITCHING" .
}
BE*/
enum start_types { START_AUTOMATIC, START_MANUAL, START_LAZY, START_ONCE };
enum run_state { CONNECTION_STOPPED, CONNECTION_RUNNING, CONNECTION_STOPPING,
				CONNECTION_STOPPING_THEN_DELETE, CONNECTION_DELETE, CONNECTION_SWITCHING};


/*BE
def BRIDGECONNECTIONS
{
	n32 ptr STRING open "name"
	n32 ptr STRINGList open "addresses"
	n32 ptr STRINGItem open "cur_address"
	n32 map bool "round_robin"
	n32 map bool "try_private"
	n32 map CONNACK_RETURN_CODES "last_connect_result"
	n32 map bool "notifications"
	n32 map START_TYPES "start_type"
	n32 map bool "stop_was_manual"
	n32 dec "cleansession"
	n32 dec "no_successful_connections"
	n32 ptr STRING open "notification_topic"
	n32 dec "keepalive_interval"
	n32 dec "inbound_filter"
	n32 ptr BRIDGETOPICSList open "topics"
	expr "topics->count" dec "number of topics"
	n32 ptr CLIENTS "primary"
	n32 ptr CLIENTS "backup"
	n32 dec "threshold"
    n32 dec "idle_timeout"
    n32 map RUN_STATE "state"
$ifdef MQTTS
    n32 map PROTOCOLS "protocol"
    n32 map bool "completed_subscriptions"
$endif
	n32 ptr STRING open "username"
	n32 ptr STRING open "password"
	n32 ptr STRING open "clientid"
}
defList(BRIDGECONNECTIONS)
BE*/
/**
 * State for each bridge connection.
 */
typedef struct
{
	char* name; 					/**< connection name */
	List* addresses;				/**< list of connection addresses */
	ListElement* cur_address;		/**< current connection address */
	unsigned int round_robin;		/**< round robin flag */
	unsigned int try_private;		/**< try private bridge connection for nanobrokers */
	unsigned int last_connect_result;	/**< did the last connection attempt work? */
	unsigned int notifications;		/**< send notifications on stop/start? */
	unsigned int start_type;		/**< automatic, manual, lazy, once */
	unsigned int stop_was_manual;	/**< manual stop means don't auto restart */
	int cleansession;				/**< clean session override flag */
	int no_successful_connections;	/**< how many successful connections have there been */
	char* notification_topic;		/**< what topic to issue the notifications on */
	int keepalive_interval;			/**< MQTT keepalive interval to use in seconds */
	int inbound_filter;				/**< not yet used */
	List* topics; 					/**< of BridgeTopics */
	Clients* primary;				/**< primary bridge client */
	Clients* backup;				/**< not needed in round robin mode */
	int threshold;					/**< lazy bridge setting */
	int idle_timeout;				/**< lazy bridge setting */
	volatile unsigned int state;	/**< run state of the connection */
#if defined(MQTTS)
	int protocol; /* 0=MQTT 1=MQTTS */
	unsigned int completed_subscriptions; /* have all the inbound topics been subscribed to? */
#endif
	char* username;                 /**< username for authenticated connections */
	char* password;                 /**< password for authenticated connections */
	char* clientid;                 /**< allow the explicit setting of clientid */
	int start_reconnect_interval;   /**< the starting, minimum retry interval after a failure */
	int max_reconnect_interval;     /**< if set, max of the range of retry intervals */
	int reconnect_interval;         /**< current value of retry interval (backs off on failure) */
	int reconnect_count;
	int chosen_reconnect_interval;
	int connect_timeout;
} BridgeConnections;

/*BE
def BRIDGES
{
	n32 ptr BRIDGECONNECTIONSList open "connections"
}
BE*/
/**
 * Bridge state
 */
typedef struct
{
	List* connections;	/**< list of connections */
} Bridges; /**< bridge state */

void Bridge_initialize(Bridges* br, SubscriptionEngines* se);
void Bridge_stop(Bridges* br);
BridgeConnections* Bridge_new_connection(Bridges* bridge, char* name);
void Bridge_terminate(Bridges* br);
void Bridge_timeslice(Bridges* bridge);
void Bridge_handleConnection(Clients* client);
int Bridge_handleConnacks(void* pack, int sock);
void Bridge_handleInbound(Clients* client, Publish* publish);
void Bridge_handleOutbound(Clients* client, Publish* publish);

#if defined(MQTTS)
BridgeConnections* Bridge_getBridgeConnection(int sock);
Clients* Bridge_getClient(int sock);
#endif

#if !defined(NO_ADMIN_COMMANDS)
int Bridge_startConnection(char* name);
int Bridge_stopConnection(char* name);
int Bridge_deleteConnection(char* name);
#endif

#endif /* BRIDGE_H */
