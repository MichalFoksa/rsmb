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

#include "StackTrace.h"
#include "Log.h"
#include "LinkedList.h"

#include "Clients.h"

#include "Broker.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "Heap.h"

/*BE
def STACKENTRY
{
	n32 ptr STRING open "name"
	n32 dec "line"
}

defList(STACKENTRY)
BE*/

#define MAX_STACK_DEPTH 30
#define MAX_FUNCTION_NAME_LENGTH 30

typedef struct
{
	char name[MAX_FUNCTION_NAME_LENGTH];
	int line;
} stackEntry;

static int maxdepth = 0;
static int current_depth = 0;

#include "StackTrace.h"

static stackEntry callstack[MAX_STACK_DEPTH];

void StackTrace_entry(const char* name, int line, int trace_level)
{
	if (trace_level != -1)
		Log_stackTrace(trace_level, 29, current_depth, name, line, NULL);
	strncpy(callstack[current_depth].name, name, sizeof(callstack[0].name)-1);
	callstack[current_depth++].line = line;
	if (current_depth > maxdepth)
		maxdepth = current_depth;
	if (current_depth >= MAX_STACK_DEPTH)
		Log(LOG_FATAL, 13, "Max stack depth exceeded");
}


void StackTrace_exit(const char* name, int line, void* rc, int trace_level)
{
	if (--current_depth < 0)
		Log(LOG_FATAL, 13, "Minimum stack depth exceeded");
	if (strncmp(callstack[current_depth].name, name, sizeof(callstack[0].name)-1) != 0)
		Log(LOG_FATAL, 13, "Stack mismatch. Entry:%s Exit:%s\n", callstack[current_depth].name, name);
	if (trace_level != -1)
		Log_stackTrace(trace_level, (rc == NULL) ? 30 : 31, current_depth, name, line, rc);
}


void StackTrace_dumpStack(char* dest)
{
	FILE* file = Log_destToFile(dest);
	int i = current_depth - 1;

	fprintf(file, "=========== Start of stack trace ==========\n");
	if (i >= 0)
	{
		fprintf(file, "%s (%d)\n", callstack[i].name, callstack[i].line);
		while (--i >= 0)
			fprintf(file, "   at %s (%d)\n", callstack[i].name, callstack[i].line);
	}
	fprintf(file, "=========== End of stack trace ==========\n\n");
	if (file != stdout && file != stderr && file != NULL)
		fclose(file);
}

