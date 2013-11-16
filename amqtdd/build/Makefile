ifndef AMQTDD_SRCDIR
  AMQTDD_SRCDIR = $(CURDIR)
endif
 
SOURCE_FILES = $(AMQTDD_SRCDIR)/*.c
HEADERS = $(AMQTDD_SRCDIR)/*.h

# In most cases the targets will be built for the native platform. However, to allow for cross compiling,
# a target platform variable can be set to override the platform choice. If this variable is set to 
# a recognised string (currently just 'Arm') the appropriate values of OSTYPE and MACHINETYPE are set.  
ifeq ($(TARGET_PLATFORM),Arm)
	OSTYPE = Linux
	MACHINETYPE = Arm
else
	ifeq ($(OS),Windows_NT)   
		OSTYPE = $(OS)
	else
		OSTYPE = $(shell uname -s)
		MACHINETYPE = $(shell uname -m)
	endif
endif

ifeq ($(OSTYPE), Windows_NT)

# the /Zp4 option aligns structure elements on 4 byte boundaries to allow
# the heapdump mapping to work

all: windows_ia32/amqtdd.exe amqtdd.ini be_amqtdd.dll

#########################################################################################################
# The following was how the DeviceDaemon was built using cygwin GNU make and has been left here as a reference 
# This has been converted to use ming GNU make (which uses the native format of file names) 
#
#windows_ia32/amqtdd.exe: $(shell cygpath -au ${SOURCE_FILES})  $(shell cygpath -au ${HEADERS})
#	-mkdir windows_ia32
#	cl -O2 -DWIN32 ws2_32.lib $(shell cygpath -am ${SOURCE_FILES}) /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Fewindows_ia32/amqtdd.exe -DMQTTMP /link /manifest
#	mt -manifest windows_ia32/amqtdd.exe.manifest -outputresource:windows_ia32/amqtdd.exe\;1
#
#amqtdd.ini:  $(shell cygpath -au ${HEADERS})
#	perl $(shell cygpath -am ${AMQTDD_SRCDIR})/tools/be/be.pl -s $(shell cygpath -am ${AMQTDD_SRCDIR}) -o amqtdd.ini
#	
#be_amqtdd.dll: be_amqtdd.obj
#	link /NOLOGO /INCREMENTAL:NO /DLL be_amqtdd.obj /OUT:$@
#
#be_amqtdd.obj: $(shell cygpath -au $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp) $(shell cygpath -au $(AMQTDD_SRCDIR)/tools/be/bememext.h)
#	cl /c /DWIN32 /D_CRT_SECURE_NO_WARNINGS /Gs /Oit /MT /nologo /W3 /WX /Tp $(shell cygpath -am $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp) /Febe_amqtdd.obj
#########################################################################################################

windows_ia32/amqtdd.exe: ${SOURCE_FILES}  ${HEADERS}
	-mkdir windows_ia32
	cl -O2 -DWIN32 ws2_32.lib ${SOURCE_FILES} /W3 /D _CRT_SECURE_NO_WARNINGS /D UNICODE /MD /Zp4 /Fewindows_ia32/amqtdd.exe -DMQTTMP /link /manifest
	mt -manifest windows_ia32/amqtdd.exe.manifest -outputresource:windows_ia32/amqtdd.exe\;1

amqtdd.ini:  ${HEADERS}
	perl ${AMQTDD_SRCDIR}/tools/be/be.pl -s ${AMQTDD_SRCDIR} -o amqtdd.ini
	
be_amqtdd.dll: be_amqtdd.obj
	link /NOLOGO /INCREMENTAL:NO /DLL be_amqtdd.obj /OUT:$@

be_amqtdd.obj: $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp $(AMQTDD_SRCDIR)/tools/be/bememext.h
	cl /c /DWIN32 /D_CRT_SECURE_NO_WARNINGS /Gs /Oit /MT /nologo /W3 /WX /Tp $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp /Febe_amqtdd.obj

endif

ifeq ($(OSTYPE),Linux)

ifeq ($(MACHINETYPE),x86_64)
	CC 	   = gcc

all: linux_ia64/amqtdd #amqtdd.ini be_amqtdd.so

linux_ia64/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir linux_ia64
	${CC} -m64 -Wall -s -Os ${SOURCE_FILES} -o linux_ia64/amqtdd -DMQTTMP
	
amqtdd.ini: ${HEADERS}
	perl ${AMQTDD_SRCDIR}/tools/be/be.pl -s ${AMQTDD_SRCDIR} -o amqtdd.ini
	
be_amqtdd.so: be_amqtdd.o
	g++ -Wall -shared -o $@ be_amqtdd.o
	chmod a-x $@

be_amqtdd.o:	$(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp $(AMQTDD_SRCDIR)/tools/be/bememext.h
	g++ -Wall -DUNIX -DLINUX -fPIC -c $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp

else 
ifeq ($(MACHINETYPE),s390x)
	CC 	   = gcc

all: linux_s390x/amqtdd #amqtdd.ini be_amqtdd.so

linux_s390x/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir linux_s390x
	${CC} -m64 -Wall -s -Os ${SOURCE_FILES} -o linux_s390x/amqtdd -DREVERSED -DMQTTMP
	
amqtdd.ini: ${HEADERS}
	perl ${AMQTDD_SRCDIR}/tools/be/be.pl -s ${AMQTDD_SRCDIR} -o amqtdd.ini
	
be_amqtdd.so: be_amqtdd.o
	g++ -Wall -shared -o $@ be_amqtdd.o
	chmod a-x $@

be_amqtdd.o:	$(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp $(AMQTDD_SRCDIR)/tools/be/bememext.h
	g++ -Wall -DUNIX -DLINUX -fPIC -c $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp

else
ifeq ($(MACHINETYPE),Arm)

	CC = gcc
	# ARM_GLIBC_CC = arm-linux-gcc
	# ARM_UCLIBC_CC = ~/x-tools/arm-unknown-linux-uclibcgnueabi/bin/arm-unknown-linux-uclibcgnueabi-gcc

	ifndef ARM_GLIBC_CC
		ARM_GLIBC_CC = arm-linux-gcc
	endif
	ifndef ARM_UCLIBC_CC
		ARM_UCLIBC_CC = arm-unknown-linux-uclibcgnueabi-gcc
	endif

all: linux_ARM_glibc/amqtdd linux_ARM_uclibc/amqtdd

linux_ARM_glibc/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir linux_ARM_glibc
	${ARM_GLIBC_CC} -Wall -s -Os ${SOURCE_FILES} -o linux_ARM_glibc/amqtdd -DMQTTMP

linux_ARM_uclibc/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir linux_ARM_uclibc	
	${ARM_UCLIBC_CC} -Wall -s -Os ${SOURCE_FILES} -o linux_ARM_uclibc/amqtdd -DMQTTMP

else

	CC = gcc

all: linux_ia32/amqtdd amqtdd.ini
#all: linux_ia32/amqtdd amqtdd.ini be_amqtdd.so

linux_ia32/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir linux_ia32
	${CC} -m32 -Wall -s -Os ${SOURCE_FILES} -o linux_ia32/amqtdd -DMQTTMP
	
amqtdd.ini: ${HEADERS}
	perl ${AMQTDD_SRCDIR}/tools/be/be.pl -s ${AMQTDD_SRCDIR} -o amqtdd.ini
	
be_amqtdd.so: be_amqtdd.o
	g++ -m32 -Wall -shared -o $@ be_amqtdd.o
	chmod a-x $@

be_amqtdd.o:	$(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp $(AMQTDD_SRCDIR)/tools/be/bememext.h
	g++ -m32 -Wall -DUNIX -DLINUX -fPIC -c $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp

endif

endif

endif

endif


ifeq ($(OSTYPE),AIX)
	CC 	   = gxlc

all: aix/amqtdd #amqtdd.ini be_amqtdd.so

aix/amqtdd: ${SOURCE_FILES} ${HEADERS}
	-mkdir aix
	${CC} -s -Os ${SOURCE_FILES} -o aix/amqtdd -DREVERSED -DMQTTMP -DAIX
	
amqtdd.ini: ${HEADERS}
	perl ${AMQTDD_SRCDIR}/tools/be/be.pl -s ${AMQTDD_SRCDIR} -o amqtdd.ini
	
be_amqtdd.so: be_amqtdd.o
	g++ -Wall -shared -o $@ be_amqtdd.o
	chmod a-x $@

be_amqtdd.o:	$(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp $(AMQTDD_SRCDIR)/tools/be/bememext.h
	g++ -Wall -DUNIX -DLINUX -fPIC -c $(AMQTDD_SRCDIR)/tools/be/be_amqtdd.cpp
	
endif
