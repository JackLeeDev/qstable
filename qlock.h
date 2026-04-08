#ifndef _qlock_h_
#define _qlock_h_

#include <stdint.h>

#define ATOM_FETCH_INC(ptr) __sync_fetch_and_add(ptr, 1)
#define ATOM_FETCH_DEC(ptr) __sync_fetch_and_sub(ptr, 1)
#define ATOM_FETCH_ADD(ptr, val) __sync_fetch_and_add(ptr, val)
#define ATOM_CAS(ptr, oval, nval) __sync_bool_compare_and_swap(ptr, oval, nval)

//spinlock
typedef volatile int32_t spinlock_t;
#define spin_lock(ptr) while(__sync_lock_test_and_set(ptr, 1)){}
#define spin_trylock(ptr) __sync_lock_test_and_set(ptr, 1)==0
#define spin_unlock(ptr) __sync_lock_release(ptr)

//rwlock
typedef volatile int32_t rwlock_t;
#define rwlock_init(ptr) *ptr=0;
#define rwlock_rlock(ptr) do { \
    while (1) { \
        int32_t val = ATOM_FETCH_INC(ptr); \
        if (val >= 0) break; \
        ATOM_FETCH_DEC(ptr); \
    }} while(0)
#define rwlock_runlock(ptr) ATOM_FETCH_DEC(ptr);
#define rwlock_wlock(ptr) do { \
    while (1) { \
        int32_t val = *ptr; \
        if (val == 0 && ATOM_CAS(ptr, 0, -1)) break; \
    }} while(0)
#define rwlock_wunlock(ptr) ATOM_CAS(ptr, -1, 0);

#endif