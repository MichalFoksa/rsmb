
ifeq ($(OS), Windows_NT)

bersmb.dll:     bersmb.obj
		link /NOLOGO /INCREMENTAL:NO /DLL bersmb.obj /OUT:$@

bersmb.obj:     bersmb.cpp bememext.h
		cl /c /DWIN32 /D_CRT_SECURE_NO_WARNINGS /Gs /Oit /MT /nologo /W3 /WX /Tp bersmb.cpp

else

bersmb.so:	bersmb.o
		g++ -Wall -shared -o $@ bersmb.o
		chmod a-x $@

bersmb.o:	bersmb.cpp bememext.h
		g++ -Wall -DUNIX -DLINUX -fPIC -c $*.cpp

endif

