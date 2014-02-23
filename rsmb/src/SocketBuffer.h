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

#if !defined(SOCKETBUFFER_H)
#define SOCKETBUFFER_H

#if defined(WIN32)
/* default on Windows is 64 - increase to make Linux and Windows the same */
#define FD_SETSIZE 1024
#include "winsock2.h"
#else
#include <sys/socket.h>
#endif

#if defined(WIN32)
	typedef WSABUF iobuf;
#else
	typedef struct iovec iobuf;
#endif

typedef struct
{
	int socket;
	int index, headerlen;
	char fixed_header[5];  // header plus up to 4 length bytes
	int buflen, // total length of the buffer
		datalen; // current length of data in buf
	char* buf;
} socket_queue;

typedef struct
{
	int socket, total, count;
	unsigned long bytes;
	iobuf iovecs[5];
} pending_writes;

#define SOCKETBUFFER_COMPLETE 0
#if !defined(SOCKET_ERROR)
	#define SOCKET_ERROR -1
#endif
#define SOCKETBUFFER_INTERRUPTED -2 /* must be the same value as TCPSOCKET_INTERRUPTED */

void SocketBuffer_initialize();
void SocketBuffer_terminate();
void SocketBuffer_cleanup(int socket);
char* SocketBuffer_getQueuedData(int socket, int bytes, int* actual_len);
int SocketBuffer_getQueuedChar(int socket, char* c);
void SocketBuffer_interrupted(int socket, int actual_len);
char* SocketBuffer_complete(int socket);
void SocketBuffer_queueChar(int socket, char c);

void SocketBuffer_pendingWrite(int socket, int count, iobuf* iovecs, int total, int bytes);
pending_writes* SocketBuffer_getWrite(int socket);
int SocketBuffer_writeComplete(int socket);
pending_writes* SocketBuffer_updateWrite(int socket, char* topic, char* payload);

#endif
