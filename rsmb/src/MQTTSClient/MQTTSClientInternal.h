/*******************************************************************************
 * Copyright (c) 2011, 2013 IBM Corp.
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


typedef struct
{
	int socket;						/**< this client's socket */
	char* addr;						/**< remote address as returned by getpeer */
	char* clientID;					/**< MQTT id of the client */
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
	List* queuedMsgs;               /**< list of queued up outbound messages - not in flight */
	int sleep_state;                /***< MQTT-S sleep state: asleep, active, awake, lost */
	List* registrations;
	PendingRegistration* pendingRegistration;
	PendingSubscription* pendingSubscription;
} Clients;
