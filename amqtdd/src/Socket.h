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

#if !defined(SOCKET_H)
#define SOCKET_H

#include <sys/types.h>

#if defined(WIN32)
/* default on Windows is 64 - increase to make Linux and Windows the same */
#define FD_SETSIZE 1024
#include <winsock2.h>
#include <ws2tcpip.h>
#define MAXHOSTNAMELEN 256
#define EAGAIN WSAEWOULDBLOCK
#define EINTR WSAEINTR
#define EINVAL WSAEINVAL
#define EINPROGRESS WSAEINPROGRESS
#define EWOULDBLOCK WSAEWOULDBLOCK
#define ENOTCONN WSAENOTCONN
#define ECONNRESET WSAECONNRESET
#define ioctl ioctlsocket
#define socklen_t int
#else
#define INVALID_SOCKET SOCKET_ERROR
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#endif

/** socket operation completed successfully */
#define TCPSOCKET_COMPLETE 0
#if !defined(SOCKET_ERROR)
	/** error in socket operation */
	#define SOCKET_ERROR -1
#endif
/** must be the same as SOCKETBUFFER_INTERRUPTED */
#define TCPSOCKET_INTERRUPTED -2
/** no further work was completed in this call */
#define TCPSOCKET_NOWORK -3

#define UDPSOCKET_INCOMPLETE 0

#if !defined(INET6_ADDRSTRLEN)
#define INET6_ADDRSTRLEN 46 /**< only needed for gcc/cygwin on windows */
#endif

#if !defined(max)
#define max(A,B) ( (A) > (B) ? (A):(B))
#endif

#include "LinkedList.h"

#if !defined(SINGLE_LISTENER)
/*BE
def SOCKADDR_IN
{
	// TODO: will be different sizes on some platforms
	16 n8 "sockaddr"
}
def SOCKADDR_IN6
{
	// TODO: will be different sizes on some platforms
	28 n8 "sockaddr6"
}
$ifndef SINGLE_LISTENER
def CONNECTION
{
	n32 dec "socket"
}
defList(CONNECTION)

def LISTENER
{
	n32 dec "socket"
	SOCKADDR_IN "addr"
	SOCKADDR_IN6 "addr6"
	n32 map bool "ipv6"
	n32 map PROTOCOLS "protocol"
	n32 ptr STRING open "address"
	n32 dec "port"
	n32 ptr CONNECTIONList open "connections"
	n32 signed dec "max_connections"
	n32 ptr STRING "mount_point"
}
defList(LISTENER)
$endif
BE*/


typedef struct
{
	int socket;
	struct sockaddr_in addr;
	struct sockaddr_in6 addr6;
	int ipv6;
	int protocol; /* 0 = MQTT, 1 = MQTTS, 2 = MQTTMP */
	char* address;
	int port;
	List* connections;
	int max_connections;
	char* mount_point;
} Listener;
#endif

/*BE
def NEWSOCKET
{
	n32 dec "socket"
	$ifdef WIN32
		n64 time "opened"
	$else
		n32 time "opened"
	$endif
	n32 dec "outbound"
}
defList(NEWSOCKET)
$endif
BE*/

typedef struct
{
	int socket;
	time_t opened;
	int outbound;
} NewSockets;

/*BE
def FD_SET
{
   128 n8 "data"
}

def SOCKETS
{
$ifndef SINGLE_LISTENER
	n32 ptr LISTENERList open "listeners"
$else
	n32 dec "mySocket"
	n32 dec "port"
	SOCKADDR_IN "addr"
	SOCKADDR_IN6 "addr6"
	n32 dec "ipv6"
$endif
	FD_SET "rset"
	FD_SET "rset_saved"
	n32 dec "maxfdp1"
	n32 ptr INTList "clientsds"
	n32 ptr INTItem "cur_clientsds"
	n32 ptr INTList "connect_pending"
	n32 ptr INTList "write_pending"
	FD_SET "pending_wset"
	n32 ptr NEWSOCKETList open "newsockets"
}
BE*/

/**
 * Structure to hold all socket data for the module
 */
typedef struct
{
#if !defined(SINGLE_LISTENER)
	List* listeners;	/**< list of listener structures */
#else
	int mySocket;		/**< server socket */
	int port;			/**< server port to listen on */
	struct sockaddr_in addr;
	struct sockaddr_in6 addr6;
	int ipv6;
#endif
	fd_set rset, /**< socket read set (see select doc) */
		rset_saved; /**< saved socket read set */
	int maxfdp1; /**< max descriptor used +1 (again see select doc) */
	List* clientsds; /**< list of client socket descriptors */
	ListElement* cur_clientsds; /**< current client socket descriptor (iterator) */
	List* connect_pending; /**< list of sockets for which a connect is pending */
	List* write_pending; /**< list of sockets for which a write is pending */
	fd_set pending_wset; /**< socket pending write set for select */
	List* newSockets; /**< sockets that haven't connected yet */
} Sockets;


#if !defined(SINGLE_LISTENER)
int Socket_initialize(List* myListeners);
int Socket_addServerSocket(Listener* listener);
Listener* Socket_new_listener();
Listener* Socket_getParentListener(int sock);
#else
int Socket_initialize(char* anAddress, int aPort, int ipv6);
#endif

int Socket_error(char* aString, int sock);

void Socket_outInitialize();
void Socket_outTerminate();
void Socket_terminate();
int Socket_getReadySocket(int more_work, struct timeval *tp);
int Socket_getch(int socket, char* c);
char *Socket_getdata(int socket, int bytes, int* actual_len);
int Socket_putdatas(int socket, char* buf0, int buf0len, int count, char** buffers, int* buflens, int* bufffree);
void Socket_close(int socket);
int Socket_new(char* addr, int port, int* socket);
#if defined(MQTTS)
int Socket_new_type(char* addr, int port, int* sock, int type);
#endif
char* Socket_gethostname();
char* Socket_getpeer(int sock);
char* Socket_getaddrname(struct sockaddr* sa, int sock);

int Socket_noPendingWrites(int socket);

typedef struct
{
	int more_work_count;
	int not_more_work_count;
	int timeout_zero_count;
	int timeout_non_zero_count;
} socket_stats;

socket_stats* Socket_getStats();

Sockets* Socket_getSockets();
NewSockets* Socket_getNew(int socket);
int Socket_removeNew(int socket);
void Socket_cleanNew(time_t now);

#endif /* SOCKET_H */
