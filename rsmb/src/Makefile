#/*******************************************************************************
# * Copyright (c) 2007, 2013 IBM Corp.
# *
# * All rights reserved. This program and the accompanying materials
# * are made available under the terms of the Eclipse Public License v1.0
# * and Eclipse Distribution License v1.0 which accompany this distribution. 
# *
# * The Eclipse Public License is available at 
# *    http://www.eclipse.org/legal/epl-v10.html
# * and the Eclipse Distribution License is available at 
# *   http://www.eclipse.org/org/documents/edl-v10.php.
# *
# * Contributors:
# *    Ian Craggs - initial API and implementation and/or initial documentation
# *******************************************************************************/

ifeq ($(OS), Windows_NT)

# the /Zp4 option aligns structure elements on 4 byte boundaries to allow
# the heapdump mapping to work

all: broker.exe broker_mqtts.exe rsmb.ini

broker.exe: *.c *.h
	vcvars32.bat
	cl -O2 -DWIN32 ws2_32.lib *.c /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Febroker.exe /link /MANIFEST
	mt -manifest broker.exe.manifest -outputresource:broker.exe\;1


broker_mqtts.exe: *.c *.h
	vcvars32.bat
	cl -O2 -DWIN32 ws2_32.lib *.c /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /DMQTTS /Zp4 /Febroker_mqtts.exe /link /MANIFEST
	mt -manifest broker_mqtts.exe.manifest -outputresource:broker_mqtts.exe\;1

rsmb.ini: *.h
	perl tools\\be\\be.pl

else

ifndef GCC
  GCC = gcc
endif

all: broker broker_dbg broker_mqtts rsmb.ini

broker: *.c *.h 
	${GCC} -Wall -s -Os *.c -o broker

broker_dbg: *.c *.h
	${GCC} -Wall -ggdb *.c -o broker_dbg

broker_mqtts: *.c *.h
	${GCC} -DMQTTS -Wall -s -Os *.c -o broker_mqtts
	
broker_mqtts32: *.c *.h
	${GCC} -m32 -DMQTTS -Wall -s -Os *.c -o broker_mqtts32

rsmb.ini: *.h
	perl tools/be/be.pl

endif

