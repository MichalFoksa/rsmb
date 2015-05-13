# Inference rules are rules distinguished by the use of the character “%” in the dependency line.
# The “%” (rule character) is a wild card, matching zero or more characters. As an example, here 
# is an inference rule for building .obj files from .c files:
#
#    %.obj : %.c
#    	$(CC) $(CFLAGS) –c $(.SOURCE)
# 
# This rule states that a .obj file can be built from a corresponding .c file when the .c and .obj 
# files share the same root of the file name. 
#
# When the source and target have the same file name except for their extensions, this rule can be 
# specified in an alternative way:
#
#    .c.obj :
#    	$(CC) $(CFLAGS) –c $(.SOURCE) 

SOURCES_MQTT=Bridge.c Broker.c Clients.c Filter.c Heap.c LinkedList.c Log.c Messages.c MQTTPacket.c MQTTPacketOut.c \
	MQTTProtocol.c MQTTProtocolClient.c MQTTProtocolOut.c Persistence.c Protocol.c Socket.c SocketBuffer.c \
	StackTrace.c SubsEngine.c Topics.c Tree.c Users.c

SOURCES_MQTT-SN=Bridge.c Broker.c Clients.c Filter.c Heap.c LinkedList.c Log.c Messages.c MQTTPacket.c MQTTPacketOut.c \
	MQTTProtocol.c MQTTProtocolClient.c MQTTProtocolOut.c MQTTSPacket.c MQTTSPacketSerialize.c MQTTSProtocol.c \
	MQTTSProtocolOut.c Persistence.c Protocol.c Socket.c SocketBuffer.c StackTrace.c SubsEngine.c Topics.c \
	Tree.c Users.c

##############################################################################
###############################    WINDOWS     ###############################
##############################################################################
ifeq ($(OS), Windows_NT)

CYGWIN_EXE_PATH = /usr/bin
RM      = $(CYGWIN_EXE_PATH)/rm.exe
MKDIR   = $(CYGWIN_EXE_PATH)/mkdir.exe

# the /Zp4 option aligns structure elements on 4 byte boundaries to allow
# the heapdump mapping to work

# TARGETS=broker.exe broker_mqtts.exe rsmb.ini
TARGETS=broker.exe broker_mqtts.exe


all: $(TARGETS)

####################    MQTT    ####################
OBJDIR=OBJ
OBJS=$(addprefix $(OBJDIR)/,$(SOURCES_MQTT:.c=.obj))

broker.exe: $(OBJS)
	vcvars32.bat
	cl -O2 -DWIN32 ws2_32.lib $^ /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Febroker.exe /link /MANIFEST
	mt -manifest broker.exe.manifest -outputresource:broker.exe\;1

$(OBJDIR)/%.obj : %.c *.h
	vcvars32.bat
	@$(MKDIR) -p $(@D)
	cl -c -O2 -DWIN32 $< /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Fo$(OBJDIR)/


####################   MQTT-S   ####################
OBJDIR_MQTT-SN=OBJ_MQTT-SN
OBJS=$(addprefix $(OBJDIR_MQTT-SN)/,$(SOURCES_MQTT-SN:.c=.obj))

broker_mqtts.exe: $(OBJS)
	vcvars32.bat
	cl -O2 -DWIN32 Iphlpapi.lib ws2_32.lib $^ /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /DMQTTS /Zp4 /Febroker_mqtts.exe /link /MANIFEST
	mt -manifest broker_mqtts.exe.manifest -outputresource:broker_mqtts.exe\;1


$(OBJDIR_MQTT-SN)/%.obj : %.c *.h
	vcvars32.bat
	@$(MKDIR) -p $(@D)
	cl -c -O2 -DWIN32 $< /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /DMQTTS /Zp4 /Fo$(OBJDIR_MQTT-SN)/


rsmb.ini: *.h
	perl tools\\be\\be.pl

clean:
	$(RM) -rf $(OBJDIR)
	$(RM) -rf $(OBJDIR_MQTT-SN)
	$(RM) -f *.manifest $(TARGETS)	

##############################################################################
############################    UNIX like     ################################
##############################################################################
else

ifndef GCC
  GCC = gcc
endif
CFLAGS=-Wall
INSTALL_PATH=~/rsmb

# TARGETS=broker broker_dbg broker_mqtts rsmb.ini
TARGETS=broker broker_mqtts

all: $(TARGETS)

####################    MQTT    ####################
OBJDIR=obj
OBJS=$(addprefix $(OBJDIR)/,$(SOURCES_MQTT:.c=.o))

broker: $(OBJS)
	$(GCC) -s -o $@ $^

$(OBJDIR)/%.o : %.c *.h
	@mkdir -p $(@D)
	$(GCC) $(CFLAGS) -Os -c $< -o $@

# broker: *.c *.h 
#	${GCC} -Wall -s -Os *.c -o broker

# broker_dbg: *.c *.h
#	${GCC} -Wall -ggdb *.c -o broker_dbg
	
####################   MQTT-S   ####################
OBJDIR_MQTT-SN=obj_mqtt-sn
OBJS=$(addprefix $(OBJDIR_MQTT-SN)/,$(SOURCES_MQTT-SN:.c=.o))

broker_mqtts: $(OBJS)
	$(GCC) -s -o $@ $^

$(OBJDIR_MQTT-SN)/%.o : %.c *.h
	@mkdir -p $(@D)
	$(GCC) $(CFLAGS) -Os -c -DMQTTS $< -o $@

# broker_mqtts: *.c *.h
#	${GCC} -DMQTTS -Wall -s -Os *.c -o broker_mqtts
	
# broker_mqtts32: *.c *.h
#	${GCC} -m32 -DMQTTS -Wall -s -Os *.c -o broker_mqtts32

rsmb.ini: *.h
	perl tools/be/be.pl

clean:
	rm -rf $(OBJDIR)
	rm -rf $(OBJDIR_MQTT-SN)
	rm -f $(TARGETS)

install: all
	for i in $(TARGETS) Messages.1.3.0.2 ; do cp $$i $(INSTALL_PATH)/$$i ; done

endif

