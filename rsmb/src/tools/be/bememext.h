/*******************************************************************************
 *
 * This file was obtained from http://www.nyangau.org/ 
 * It is part of Andy's Binary Folding Editor
 * package, which is in the public domain.  It should not be changed.
 *
 *******************************************************************************/

//
// bememext.h - Interface needed by implementors of BE memory extensions
//
// Despite both BE and the helpers being implemented in C++, I use a C
// style interface. This avoids name mangling problems.
//
// The bemem_init entrypoint will be called before any other entrypoint.
// If bemem_init fails, then it should set err to point to some meaningful
// static error string, and return FALSE.
// bemem_init can be omitted if there is no initialisation required.
//
// For every non-0 bemem_create, BE will later call bemem_delete.
// If bemem_create fails, then it should set err to point to some meaningful
// static error string, and return (void *) 0.
//
// After all bemem_deletes, bemem_term will be called (last).
// bemem_term can be omitted if there is no termination required.
//
// If the memory extension helper is caching data (presumably for speed),
// then it should discard this cache if bemem_refresh is called.
// If a memory extension helper does not cache any data, it need not
// implement a bemem_refresh routine.
//
// A memory extension helper can optionally provide the bemem_write and
// bemem_flush routines, if the data it provides access to is in some way
// modifiable. bemem_write changes a byte in the data, and before the
// Binary Editor shuts down, it will call bemem_flush to make any changes
// made using bemem_write 'final'. If data is modified via bemem_write,
// the modified data should immediately be accessible via bemem_read.
//
// bemem_options is an optional mechanism whereby arbitrary commands may
// be passed through to the memory extension, for a specific instance.
//
// If 64 bit address space entrypoints are implemented, then BE will use
// these, otherwise it will call the older 32 bit entrypoints.
//
// If a memory extension implements bemem_memacc and/or bemem_memacc_64,
// then immediately after calling bemem_init, BE will call the extension to
// tell it the address of functions within BE itself which access the BE whole
// memory space. In this way memory extensions may be written which provide
// shadow views of data elsewhere in the memory space, or data derived from
// data elsewhere in the memory space.
//

#ifndef BEMEM_H
#define	BEMEM_H

#ifndef Boolean_DEFINED
#define	Boolean_DEFINED
typedef int Boolean;
#define	TRUE  1
#define FALSE 0
#endif

#if   defined(OS2)
#define	BEMEMEXPORT
#define	BEMEMENTRY _System
#define BEMEMADDR32 unsigned
#define BEMEMADDR64 unsigned_long_long
#elif defined(WIN32)
#define	BEMEMEXPORT __declspec(dllexport)
#define	BEMEMENTRY __stdcall
#define BEMEMADDR32 unsigned
#define BEMEMADDR64 unsigned __int64
#elif defined(DOS32)
#define	BEMEMEXPORT 
#define	BEMEMENTRY __export _cdecl
#define BEMEMADDR32 unsigned
#define BEMEMADDR64 unsigned __int64
#elif defined(NW)
#define	BEMEMEXPORT
#define	BEMEMENTRY
#define BEMEMADDR32 unsigned
#define BEMEMADDR64 unsigned __int64
#else
#define	BEMEMEXPORT
#define	BEMEMENTRY
#define BEMEMADDR32 unsigned
#define BEMEMADDR64 unsigned long long
#endif

#ifdef AIX
typedef void (*BEMEM_EP)(void);
typedef struct { BEMEM_EP ep; const char *name; } BEMEM_EXPORT;
#endif

typedef Boolean (*BEMEMREAD32)(BEMEMADDR32 addr, unsigned char & b);
typedef Boolean (*BEMEMWRITE32)(BEMEMADDR32 addr, unsigned char b);
#ifdef BE64
typedef Boolean (*BEMEMREAD64)(BEMEMADDR64 addr, unsigned char & b);
typedef Boolean (*BEMEMWRITE64)(BEMEMADDR64 addr, unsigned char b);
#endif

extern "C" {

BEMEMEXPORT Boolean BEMEMENTRY bemem_init(const char *(&err));
BEMEMEXPORT void *  BEMEMENTRY bemem_create(const char *args, BEMEMADDR32 addr, const char *(&err));
BEMEMEXPORT Boolean BEMEMENTRY bemem_read(void * ptr, BEMEMADDR32 addr, unsigned char & b);
BEMEMEXPORT Boolean BEMEMENTRY bemem_write(void * ptr, BEMEMADDR32 addr, unsigned char b);
BEMEMEXPORT void    BEMEMENTRY bemem_memacc(BEMEMREAD32 read_fn, BEMEMWRITE32 write_fn);
#ifdef BE64
BEMEMEXPORT void *  BEMEMENTRY bemem_create_64(const char *args, BEMEMADDR64 addr, const char *(&err));
BEMEMEXPORT Boolean BEMEMENTRY bemem_read_64(void * ptr, BEMEMADDR64 addr, unsigned char & b);
BEMEMEXPORT Boolean BEMEMENTRY bemem_write_64(void * ptr, BEMEMADDR64 addr, unsigned char b);
BEMEMEXPORT void    BEMEMENTRY bemem_memacc_64(BEMEMREAD64 read_fn, BEMEMWRITE64 write_fn);
#endif
BEMEMEXPORT void    BEMEMENTRY bemem_refresh(void * ptr);
BEMEMEXPORT Boolean BEMEMENTRY bemem_flush(void * ptr);
BEMEMEXPORT Boolean BEMEMENTRY bemem_options(void * ptr, const char *options, const char *(&err));
BEMEMEXPORT void    BEMEMENTRY bemem_delete(void * ptr);
BEMEMEXPORT void    BEMEMENTRY bemem_term();

}

#endif
