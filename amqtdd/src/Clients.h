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


#if !defined(CLIENTS_H)
#define CLIENTS_H

#include <time.h>
#include "LinkedList.h"
#include "Users.h"
#if !defined(PRIORITY_MAX)
#define PRIORITY_MAX 3
#endif

/*BE
include "LinkedList"
include "Users"
BE*/

/*BE
map PROTOCOLS
{
   "mqtt" .
   "mqtts" .
   "mqttmp" .
}
BE*/

/*BE
def PUBLICATIONS
{
   n32 ptr STRING open "topic"
   n32 ptr DATA "payload"
   n32 dec "payloadlen"
   n32 dec "refcount"
}
BE*/
/**
 * Stored publication data to minimize copying
 */
typedef struct
{
	char *topic;
	char* payload;
	int payloadlen;
	int refcount;
} Publications;

/*BE
// This should get moved to MQTTProtocol, but the includes don't quite work yet
map MESSAGE_TYPES
{
   "PUBREC" 5
   "PUBREL" .
   "PUBCOMP" .
}


def MESSAGES
{
   n32 dec "qos"
   n32 map bool "retain"
   n32 dec "msgid"
   n32 dec "priority"
   n32 ptr PUBLICATIONS "publish"
$ifdef WIN32
   n64 time "lastTouch"
$else
   n32 time "lastTouch"
$endif
   n8 map MESSAGE_TYPES "nextMessageType"
   n32 dec "len"
}
defList(MESSAGES)
BE*/
/**
 * Client publication message data
 */
typedef struct
{
	int qos;
	int retain;
	int msgid;
	int priority;
	Publications *publish;
	time_t lastTouch; /* used for retry and expiry */
	char nextMessageType; /* PUBREC, PUBREL, PUBCOMP */
	int len; /* length of the whole structure+data */
} Messages;


/*BE
def WILLMESSAGES
{
   n32 ptr STRING open "topic"
   n32 ptr DATA open "msg"
   n32 dec "retained"
   n32 dec "qos"
}
BE*/
/**
 * Client will message data
 */
typedef struct
{
	char *topic;
	char *msg;
	int retained;
	int qos;
} willMessages;

#if defined(MQTTS)
/*BE
$ifdef MQTTS
BE*/

/*BE
def REGISTRATION
{
   n32 dec "id"
   n32 ptr STRING open "topicName"
}
defList(REGISTRATION)
BE*/
typedef struct
{
	int id;
	char* topicName;
} Registration;

/*BE
def PENDINGREGISTRATION
{
   n32 ptr REGISTRATION "registration"
   n32 dec "msgId"
$ifdef WIN32
   n64 time "sent"
$else
   n32 time "sent"
$endif
}
BE*/
typedef struct
{
	Registration* registration;
	int msgId;
	time_t sent;
} PendingRegistration;

#if !defined(NO_BRIDGE)
/*BE
$ifndef NO_BRIDGE
def PENDINGSUBSCRIPTION
{
   n32 ptr STRING open "topicName"
   n32 dec "qos"
   n32 dec "msgId"
$ifdef WIN32
   n64 time "sent"
$else
   n32 time "sent"
$endif
}
$endif
BE*/
typedef struct
{
	char* topicName;
	int qos;
	int msgId;
	time_t sent;
} PendingSubscription;

/*BE
$endif
BE*/
#endif

/*BE
$endif
BE*/
#endif

/*BE
map CLIENT_BITS
{
	"cleansession" 1 : .
	"connected" 2 : .
	"good" 4 : .
	"outbound"  8 : .
	"noLocal" 16 : .
	"ping_outstanding" 32 : .
}
def CLIENTS
{
	n32 dec suppress "socket"
	n32 ptr STRING open suppress "addr"
	n32 ptr STRING open "clientID"
	n32 ptr USER suppress "user"
	n32 ptr STRING open suppress "username (outbound)"
	n32 ptr STRING open suppress "password (outbound)"
	n32 map CLIENT_BITS "bits"
	n32 dec suppress "msgID"
	n32 dec suppress "keepAliveInterval"
	n32 ptr BRIDGECONNECTIONS suppress "bridge_context"
$ifdef WIN32
	n64 time suppress "lastContact"
$else
	n32 time suppress "lastContact"
$endif
	n32 ptr WILLMESSAGES suppress "will"
	n32 ptr MESSAGESList open suppress "inboundMsgs"
	n32 ptr MESSAGESList open suppress "outboundMsgs"
	n32 ptr MESSAGESList open suppress "queuedMsgs"
	n32 dec suppress "discardedMsgs"
$ifndef MQTTS MQTTMP
  // null statement - logic to achieve "if MQTTS or MQTTMP"
$else
    n32 map PROTOCOLS "protocol"
$endif
$ifdef MQTTMP
    n32 dec suppress "channel"
    n32 dec suppress "actualSocket"
$endif
$ifdef MQTTS
	n32 ptr REGISTRATIONList open suppress "registrations"
	n32 ptr PENDINGREGISTRATION suppress "pendingRegistration"
$ifndef NO_BRIDGE
	n32 ptr PENDINGSUBSCRIPTION suppress "pendingSubscription"
$endif
$endif
}

defList(CLIENTS)

BE*/
/**
 * The information and state for each client.
 */
typedef struct
{
	int socket;						/**< this client's socket */
	char* addr;						/**< remote address as returned by getpeer */
	char* clientID;					/**< MQTT id of the client */
	User* user;						/**< Authenticated user object for this client */
	char* username;                 /**< Username for outbound client connections */
	char* password;                 /**< Password for outbound client connections */
	unsigned int cleansession : 1;	/**< MQTT cleansession flag */
	unsigned int connected : 1;		/**< is the client connected */
	unsigned int good : 1;			/**< if we have an error on the socket we turn this off */
	unsigned int outbound : 1;		/**< is this an incoming, or bridge client */
	unsigned int noLocal : 1;		/**< subscriptions to be noLocal? */
	unsigned int ping_outstanding : 1;	/**< have we sent a ping? */
	unsigned int connect_state : 2;		/**< state while connecting */
	int msgID;						/**< current outward MQTT message id */
	int keepAliveInterval;			/**< MQTT keep alive interval in seconds */
	void* bridge_context; 			/**< for bridge use */
	time_t lastContact;				/**< time of last contact with this client */
	willMessages* will;				/**< will message if set (NULL if not) */
	List* inboundMsgs;				/**< list of inbound message state */
	List* outboundMsgs;				/**< list of outbound in flight messages */
	List* queuedMsgs[PRIORITY_MAX]; /**< list of queued up outbound messages - not in flight */
	int discardedMsgs;				/**< how many have we had to throw away? */
#if defined(MQTTS) || defined(MQTTMP)
	int protocol; /* 0=MQTT 1=MQTTS 2=MQTT_MP */
#endif
	#define PROTOCOL_MQTT 0
#if defined(MQTTMP)
	#define PROTOCOL_MQTT_MP 2
	int channel;					/**< the multiplexed channel the client is on */
	int actualSock;					/**< the real socket the client is on, if it is a multiplexed client */
#endif
#if defined(MQTTS)
	#define PROTOCOL_MQTTS 1
	List* registrations;
	PendingRegistration* pendingRegistration;
#if !defined(NO_BRIDGE)
	PendingSubscription* pendingSubscription;
#endif
#endif
} Clients;
#pragma pack()
int clientIDCompare(void* a, void* b);
int clientSocketCompare(void* a, void* b);
int queuedMsgsCount(Clients*);

#if defined(MQTTS) || defined(MQTTMP)
int clientAddrCompare(void*a, void* b);
#endif

#endif
