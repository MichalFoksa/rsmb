ifeq ($(OS), Windows_NT)

# the /Zp4 option aligns structure elements on 4 byte boundaries to allow
# the heapdump mapping to work

# TARGETS=broker.exe broker_mqtts.exe rsmb.ini
TARGETS=broker_mqtts.exe

all: $(TARGETS)

broker.exe: *.c *.h
	vcvars32.bat
	cl -O2 -DWIN32 ws2_32.lib *.c /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Febroker.exe /link /MANIFEST
	mt -manifest broker.exe.manifest -outputresource:broker.exe\;1


broker_mqtts.exe: *.c *.h
	vcvars32.bat
	cl -O2 -DWIN32 Iphlpapi.lib ws2_32.lib *.c /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /DMQTTS /Zp4 /Febroker_mqtts.exe /link /MANIFEST
	mt -manifest broker_mqtts.exe.manifest -outputresource:broker_mqtts.exe\;1

rsmb.ini: *.h
	perl tools\\be\\be.pl

CYGWIN_EXE_PATH = /usr/bin
RM        = $(CYGWIN_EXE_PATH)/rm.exe

clean:
	$(RM) -f *.obj *.manifest broker.exe broker_mqtts.exe


else

# TARGETS=broker broker_dbg broker_mqtts rsmb.ini
TARGETS=broker_mqtts

INSTALL_PATH?=~/bin

ifndef GCC
  GCC = gcc
endif

all: $(TARGETS)

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

clean:
	rm -f *.o broker broker_dbg broker_mqtts

install: all
	for i in $(TARGETS) ; do cp $$i $(INSTALL_PATH)/$$i ; done

endif

