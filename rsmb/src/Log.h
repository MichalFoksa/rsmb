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
#if !defined(LOG_H)
#define LOG_H

/*BE
map LOG_LEVELS
{
	"TRACE_MAXIMUM" 1
	"TRACE_MEDIUM" 2
	"TRACE_MINIMUM" 3
	"TRACE_PROTOCOL" 4

	"CONFIG" 5
	"DETAIL" 6
	"INFO" 7
	"AUDIT" 8
	"WARNING" 9
	"ERROR" 10
	"SEVERE" 11
	"FATAL" 12
}
BE*/

enum {
	TRACE_MAXIMUM = 1,
	TRACE_MEDIUM,
	TRACE_MINIMUM,
	TRACE_PROTOCOL,
	LOG_CONFIG,
	LOG_DETAIL,
	LOG_INFO,
	LOG_INFORMATION = LOG_INFO,
	LOG_AUDIT,
	LOG_WARNING,
	LOG_WARN = LOG_WARNING,
	LOG_ERROR,
	LOG_SEVERE,
	LOG_FATAL,
} Log_levels;


/*BE
def trace_settings_type
{
   n32 map LOG_LEVELS "log_level"
   n32 dec "max_log_entries"
   n32 map LOG_LEVELS "trace_level"
   n32 dec "max_trace_entries"
   n32 dec "trace_output_level"
   n32 dec "isservice"
}
BE*/
typedef struct
{
	int log_level;				/**< log level */
	int max_log_entries;        /**< max no of entries in the log buffer */
	int trace_level;			/**< trace level */
	int max_trace_entries;		/**< max no of entries in the trace buffer */
	int trace_output_level;		/**< trace level to output to destination */
	int isdaemon;				/**< are we running as Linux daemon, or Windows service? */
} trace_settings_type;

extern trace_settings_type trace_settings;

#define LOG_PROTOCOL TRACE_PROTOCOL
#define TRACE_MAX TRACE_MAXIMUM
#define TRACE_MIN TRACE_MINIMUM
#define TRACE_MED TRACE_MEDIUM

#define MSG_PREFIX "CWNAN"

int Log_initialize();
void Log_terminate();

void Log_setPublish(int flag);

void Log(int, int, char *, ...);
void Log_stackTrace(int, int, int, const char*, int, int*);

#include <stdio.h>

FILE* Log_destToFile(char* dest);
int Log_dumpTrace(char *);

int Log_traceOutput(char*);

#endif
