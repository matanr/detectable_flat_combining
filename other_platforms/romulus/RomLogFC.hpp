/*-
 * Copyright 2019-2020
 *   Andreia Correia <andreia.veiga@unine.ch>
 *   Pedro Ramalhete <pramalhe@gmail.com>
 *   Pascal Felber <pascal.felber@unine.ch>
 *
 * This work is published under the MIT license. See LICENSE.txt
 */
#ifndef _ROMULUS_LOG_FLAT_COMBINING_H_
#define _ROMULUS_LOG_FLAT_COMBINING_H_

#include <atomic>
#include <cassert>
#include <functional>
#include <cstring>
#include <thread>       // Needed by this_thread::yield()
#include <sys/mman.h>   // Needed if we use mmap()
#include <sys/types.h>  // Needed by open() and close()
#include <sys/stat.h>
#include <linux/mman.h> // Needed by MAP_SHARED_VALIDATE
#include <fcntl.h>
#include <unistd.h>     // Needed by close()
#include <type_traits>

#include <libpmem.h>

/*
 * <h1> Romulus Log </h1>
 * This is a special version of Romulus Log that is meant for comparing in the sequential SPS.
 * It is a single file (this one) and uses the EsLoco allocator
 */


// Macros needed for persistence
#if defined(PWB_IS_CLFLUSH_PFENCE_NOP)
  /*
   * More info at http://elixir.free-electrons.com/linux/latest/source/arch/x86/include/asm/special_insns.h#L213
   * Intel programming manual at https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
   * Use these for Broadwell CPUs (cervino server)
   */
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")                      // Broadwell only works with this.
  #define PFENCE()               {}                                                                             // No ordering fences needed for CLFLUSH (section 7.4.6 of Intel manual)
  #define PSYNC()                {}                                                                             // For durability it's not obvious, but CLFLUSH seems to be enough, and PMDK uses the same approach
#elif defined(PWB_IS_CLFLUSH)
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif defined(PWB_IS_CLWB)
  /* Use this for CPUs that support clwb, such as the SkyLake SP series (c5 compute intensive instances in AWS are an example of it) */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))  // clwb() only for Ice Lake onwards
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif defined(PWB_IS_NOP)
  /* pwbs are not needed for shared memory persistency (i.e. persistency across process failure) */
  #define PWB(addr)              {}
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif defined(PWB_IS_CLFLUSHOPT)
  /* Use this for CPUs that support clflushopt, which is most recent x86 */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))    // clflushopt (Kaby Lake)
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif defined(PWB_IS_PMEM)
  #define PWB(addr)              pmem_flush(addr, sizeof(addr))
  #define PFENCE()               pmem_drain()
  #define PSYNC() 				 pmem_drain()
#elif defined(COUNT_PWB)
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory") ; localPwbCounter++
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory") ; localPfenceCounter++
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory") ; localPfenceCounter++
//   #define PSYNC()                __asm__ volatile("sfence" : : : "memory") ; localPsyncCounter++
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSHOPT if you don't know what your CPU is capable of"
#endif


#if defined(COUNT_PWB)
thread_local int localPwbCounter = 0;
thread_local int localPfenceCounter = 0;
thread_local int localPsyncCounter = 0;
#endif


namespace romlogfc {

//
// User configurable variables.
// Feel free to change these if you need larger transactions, or more threads.
//

// Start address of persistent memory region
#ifndef PM_REGION_BEGIN
#define PM_REGION_BEGIN 0x7fea00000000
#endif
// Size of the persistent memory region
#ifndef PM_REGION_SIZE
#define PM_REGION_SIZE 1024*1024*1024ULL // 1GB for now
// #define PM_REGION_SIZE 1024*1024*128ULL 
#endif
// Name of persistent file mapping
#ifndef PM_FILE_NAME
// #define PM_FILE_NAME   "/home/matanr/recov_flat_combining/other_platforms/shm/romlogfc_shared"
#define PM_FILE_NAME   "/mnt/dfcpmem/romlogfc_shared"
#endif

// Maximum number of registered threads that can execute transactions
static const int REGISTRY_MAX_THREADS = 128;
// Start address of persistent memory
static uint8_t* PREGION_ADDR = (uint8_t*)0x7fea00000000;
// End address of mapped persistent memory
static uint8_t* PREGION_END = (PREGION_ADDR+PM_REGION_SIZE);
// Main start
static uint8_t* PMAIN_ADDR = PREGION_ADDR;
// Main end
static uint8_t* PMAIN_END = PMAIN_ADDR + PM_REGION_SIZE/2;
// Main start
static uint8_t* PBACK_ADDR = PMAIN_END;
// Main end
static uint8_t* PBACK_END = PBACK_ADDR + PM_REGION_SIZE/2;
// Maximum number of root pointers available for the user
static const uint64_t MAX_ROOT_POINTERS = 64;


// Flush each cache line in a range
static inline void flushFromTo(void* from, void* to) noexcept {
    const uint64_t cache_line_size = 64;
    uint8_t* ptr = (uint8_t*)(((uint64_t)from) & (~(cache_line_size-1)));
    for (; ptr < (uint8_t*)to; ptr += cache_line_size) {PWB(ptr);}
}


//
// Thread Registry stuff
//
extern void thread_registry_deregister_thread(const int tid);

// An helper class to do the checkin and checkout of the thread registry
struct ThreadCheckInCheckOut {
    static const int NOT_ASSIGNED = -1;
    int tid { NOT_ASSIGNED };
    ~ThreadCheckInCheckOut() {
        if (tid == NOT_ASSIGNED) return;
        thread_registry_deregister_thread(tid);
    }
};

extern thread_local ThreadCheckInCheckOut tl_tcico;

// Forward declaration of global/singleton instance
class ThreadRegistry;
extern ThreadRegistry gThreadRegistry;

/*
 * <h1> Registry for threads </h1>
 *
 * This is singleton type class that allows assignement of a unique id to each thread.
 * The first time a thread calls ThreadRegistry::getTID() it will allocate a free slot in 'usedTID[]'.
 * This tid wil be saved in a thread-local variable of the type ThreadCheckInCheckOut which
 * upon destruction of the thread will call the destructor of ThreadCheckInCheckOut and free the
 * corresponding slot to be used by a later thread.
 */
class ThreadRegistry {
private:
    alignas(128) std::atomic<bool>      usedTID[REGISTRY_MAX_THREADS];   // Which TIDs are in use by threads
    alignas(128) std::atomic<int>       maxTid {-1};                     // Highest TID (+1) in use by threads

public:
    ThreadRegistry() {
        for (int it = 0; it < REGISTRY_MAX_THREADS; it++) {
            usedTID[it].store(false, std::memory_order_relaxed);
        }
    }

    // Progress condition: wait-free bounded (by the number of threads)
    int register_thread_new(void) {
        for (int tid = 0; tid < REGISTRY_MAX_THREADS; tid++) {
            if (usedTID[tid].load(std::memory_order_acquire)) continue;
            bool unused = false;
            if (!usedTID[tid].compare_exchange_strong(unused, true)) continue;
            // Increase the current maximum to cover our thread id
            int curMax = maxTid.load();
            while (curMax <= tid) {
                maxTid.compare_exchange_strong(curMax, tid+1);
                curMax = maxTid.load();
            }
            tl_tcico.tid = tid;
            return tid;
        }
        printf("ERROR: Too many threads, registry can only hold %d threads\n", REGISTRY_MAX_THREADS);
        assert(false);
    }

    // Progress condition: wait-free population oblivious
    inline void deregister_thread(const int tid) {
        usedTID[tid].store(false, std::memory_order_release);
    }

    // Progress condition: wait-free population oblivious
    static inline uint64_t getMaxThreads(void) {
        return gThreadRegistry.maxTid.load(std::memory_order_acquire);
    }

    // Progress condition: wait-free bounded (by the number of threads)
    static inline int getTID(void) {
        int tid = tl_tcico.tid;
        if (tid != ThreadCheckInCheckOut::NOT_ASSIGNED) return tid;
        return gThreadRegistry.register_thread_new();
    }
};


// T is typically a pointer to a node, but it can be integers or other stuff, as long as it fits in 64 bits
template<typename T> struct persist {
    uint64_t val;

    persist() { }

    persist(T initVal) { pstore(initVal); }

    // Casting operator
    operator T() { return pload(); }

    // Casting to const
    operator T() const { return pload(); }

    // Prefix increment operator: ++x
    void operator++ () { pstore(pload()+1); }
    // Prefix decrement operator: --x
    void operator-- () { pstore(pload()-1); }
    void operator++ (int) { pstore(pload()+1); }
    void operator-- (int) { pstore(pload()-1); }

    // Equals operator
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator == (const persist<Y> &rhs) { return pload() == rhs; }
    // Difference operator: first downcast to T and then compare
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator != (const persist<Y> &rhs) { return pload() != rhs; }
    // Relational operators
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator < (const persist<Y> &rhs) { return pload() < rhs; }
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator > (const persist<Y> &rhs) { return pload() > rhs; }
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator <= (const persist<Y> &rhs) { return pload() <= rhs; }
    template <typename Y, typename = typename std::enable_if<std::is_convertible<Y, T>::value>::type>
    bool operator >= (const persist<Y> &rhs) { return pload() >= rhs; }

    // Operator arrow ->
    T operator->() { return pload(); }

    // Copy constructor
    persist<T>(const persist<T>& other) { pstore(other.pload()); }

    // Assignment operator from an tmtype
    persist<T>& operator=(const persist<T>& other) {
        pstore(other.pload());
        return *this;
    }

    // Assignment operator from a value
    persist<T>& operator=(T value) {
        pstore(value);
        return *this;
    }

    // Operator &
    T* operator&() {
        return (T*)this;
    }

    // Meant to be called when know we're the only ones touching
    // these contents, for example, in the constructor of an object, before
    // making the object visible to other threads.
    inline void isolated_store(T newVal) {
        val = (uint64_t)newVal;
    }

    // There is no load interposing in romulus log
    inline T pload() const { return (T)val; }

    // Methods that are defined later because they have compilation dependencies
    inline void pstore(T newVal);
};


/*
 * EsLoco is an Extremely Simple memory aLOCatOr
 *
 * It is based on intrusive singly-linked lists (a free-list), one for each power of two size.
 * All blocks are powers of two, the smallest size enough to contain the desired user data plus the block header.
 * There is an array named 'freelists' where each entry is a pointer to the head of a stack for that respective block size.
 * Blocks are allocated in powers of 2 of words (64bit words).
 * Each block has an header with two words: the size of the node (in words), the pointer to the next node.
 * The minimum block size is 4 words, with 2 for the header and 2 for the user.
 * When there is no suitable block in the freelist, it will create a new block from the remaining pool.
 *
 * EsLoco was designed for usage in PTMs but it doesn't have to be used only for that.
 * Average number of stores for an allocation is 1.
 * Average number of stores for a de-allocation is 2.
 *
 * Memory layout:
 * ------------------------------------------------------------------------
 * | poolTop | freelists[0] ... freelists[61] | ... allocated objects ... |
 * ------------------------------------------------------------------------
 */
template <template <typename> class P>
class EsLoco {
private:
    struct block {
        P<block*>   next;   // Pointer to next block in free-list (when block is in free-list)
        P<uint64_t> size;   // Exponent of power of two of the size of this block in bytes.
    };

    const bool debugOn = false;

    // Volatile data
    uint8_t* poolAddr {nullptr};
    uint64_t poolSize {0};

    // Pointer to array of persistent heads of free-list
    block* freelists {nullptr};
    // Volatile pointer to persistent pointer to last unused address (the top of the pool)
    P<uint8_t*>* poolTop {nullptr};

    // Number of blocks in the freelists array.
    // Each entry corresponds to an exponent of the block size: 2^4, 2^5, 2^6... 2^40
    static const int kMaxBlockSize = 40; // 1 TB of memory should be enough

    // For powers of 2, returns the highest bit, otherwise, returns the next highest bit
    uint64_t highestBit(uint64_t val) {
        uint64_t b = 0;
        while ((val >> (b+1)) != 0) b++;
        if (val > (1 << b)) return b+1;
        return b;
    }

    uint8_t* aligned(uint8_t* addr) {
        return (uint8_t*)((size_t)addr & (~0x3FULL)) + 128;
    }

public:
    void init(void* addressOfMemoryPool, size_t sizeOfMemoryPool, bool clearPool=true) {
        // Align the base address of the memory pool
        poolAddr = aligned((uint8_t*)addressOfMemoryPool);
        poolSize = sizeOfMemoryPool + (uint8_t*)addressOfMemoryPool - poolAddr;
        // The first thing in the pool is a pointer to the top of the pool
        poolTop = (P<uint8_t*>*)poolAddr;
        // The second thing in the pool is the array of freelists
        freelists = (block*)(poolAddr + sizeof(*poolTop));
        if (clearPool) {
            std::memset(poolAddr, 0, poolSize);
            for (int i = 0; i < kMaxBlockSize; i++) freelists[i].next.pstore(nullptr);
            // The size of the freelists array in bytes is sizeof(block)*kMaxBlockSize
            // Align to cache line boundary (DCAS needs 16 byte alignment)
            poolTop->pstore(aligned(poolAddr + sizeof(*poolTop) + sizeof(block)*kMaxBlockSize));
        }
        if (debugOn) printf("Starting EsLoco with poolAddr=%p and poolSize=%ld, up to %p\n", poolAddr, poolSize, poolAddr+poolSize);
    }

    // Resets the metadata of the allocator back to its defaults
    void reset() {
        std::memset(poolAddr, 0, sizeof(block)*kMaxBlockSize);
        poolTop->pstore(nullptr);
    }

    // Returns the number of bytes that may (or may not) have allocated objects, from the base address to the top address
    uint64_t getUsedSize() {
        return poolTop->pload() - poolAddr;
    }

    // Takes the desired size of the object in bytes.
    // Returns pointer to memory in pool, or nullptr.
    // Does on average 1 store to persistent memory when re-utilizing blocks.
    void* malloc(size_t size) {
        P<uint8_t*>* top = (P<uint8_t*>*)(((uint8_t*)poolTop));
        block* flists = (block*)(((uint8_t*)freelists));
        // Adjust size to nearest (highest) power of 2
        uint64_t bsize = highestBit(size + sizeof(block));
        if (debugOn) printf("malloc(%ld) requested,  block size exponent = %ld\n", size, bsize);
        block* myblock = nullptr;
        // Check if there is a block of that size in the corresponding freelist
        if (flists[bsize].next.pload() != nullptr) {
            if (debugOn) printf("Found available block in freelist\n");
            // Unlink block
            myblock = flists[bsize].next;
            flists[bsize].next = myblock->next;          // pstore()
        } else {
            if (debugOn) printf("Creating new block from top, currently at %p\n", top->pload());
            // Couldn't find a suitable block, get one from the top of the pool if there is one available
            if (top->pload() + (1<<bsize) > poolSize + poolAddr) return nullptr;
            myblock = (block*)top->pload();
            top->pstore(top->pload() + (1<<bsize));      // pstore()
            myblock->size = bsize;                       // pstore()
        }
        if (debugOn) printf("returning ptr = %p\n", (void*)((uint8_t*)myblock + sizeof(block)));
        // Return the block, minus the header
        return (void*)((uint8_t*)myblock + sizeof(block));
    }

    // Takes a pointer to an object and puts the block on the free-list.
    // Does on average 2 stores to persistent memory.
    void free(void* ptr) {
        if (ptr == nullptr) return;
        block* flists = (block*)(((uint8_t*)freelists));
        block* myblock = (block*)((uint8_t*)ptr - sizeof(block));
        if (debugOn) printf("free(%p)  block size exponent = %ld\n", ptr, myblock->size.pload());
        // Insert the block in the corresponding freelist
        myblock->next = flists[myblock->size].next;      // pstore()
        flists[myblock->size].next = myblock;            // pstore()
    }
};


// Needed by our benchmarks
struct tmbase {
};


// Pause to prevent excess processor bus usage
#if defined( __sparc )
#define Pause() __asm__ __volatile__ ( "rd %ccr,%g0" )
#elif defined( __i386 ) || defined( __x86_64 )
#define Pause() __asm__ __volatile__ ( "pause" : : : )
#else
#define Pause() std::this_thread::yield();
#endif


/**
 * <h1> C-RW-WP </h1>
 *
 * A C-RW-WP reader-writer lock with writer preference and using a
 * Ticket Lock as Cohort.
 * This is starvation-free for writers and for readers, but readers may be
 * starved by writers.
 * C-RW-WP paper:         http://dl.acm.org/citation.cfm?id=2442532
 *
 * This variant of C-RW-WP has two modes on the writersMutex so that readers can
 * enter (but not writers). This is specific to DualZone because it's ok to let
 * the readers enter the 'main' while we're replicating modifications on 'back'.
 *
 */
class CRWWPSpinLock {

private:
    class SpinLock {
        alignas(128) std::atomic<int> writers {0};
    public:
        inline bool isLocked() { return (writers.load() != 0); }
        inline void lock() {
            while (!tryLock()) Pause();
        }
        inline bool tryLock() {
            if (writers.load() != 0) return false;
            int tmp = 0;
            #if defined(COUNT_PWB)
                localPfenceCounter++;
            #endif
            return writers.compare_exchange_strong(tmp,2);
        }
        inline void unlock() {
            writers.store(0, std::memory_order_release);
        }
    };

    class RIStaticPerThread {
    private:
        static const uint64_t NOT_READING = 0;
        static const uint64_t READING = 1;
        static const int CLPAD = 128/sizeof(uint64_t);
        alignas(128) std::atomic<uint64_t>* states;

    public:
        RIStaticPerThread() {
            states = new std::atomic<uint64_t>[REGISTRY_MAX_THREADS*CLPAD];
            for (int tid = 0; tid < REGISTRY_MAX_THREADS; tid++) {
                states[tid*CLPAD].store(NOT_READING, std::memory_order_relaxed);
            }
        }

        ~RIStaticPerThread() {
            delete[] states;
        }

        inline void arrive(const int tid) noexcept {
            states[tid*CLPAD].store(READING);
        }

        inline void depart(const int tid) noexcept {
            states[tid*CLPAD].store(NOT_READING, std::memory_order_release);
        }

        inline bool isEmpty() noexcept {
            const int maxTid = ThreadRegistry::getMaxThreads();
            for (int tid = 0; tid < maxTid; tid++) {
                if (states[tid*CLPAD].load() != NOT_READING) return false;
            }
            return true;
        }
    };

    SpinLock splock {};
    RIStaticPerThread ri {};

public:
    std::string className() { return "C-RW-WP-SpinLock"; }

    inline void exclusiveLock() {
        splock.lock();
        while (!ri.isEmpty()) Pause();
    }

    inline bool tryExclusiveLock() {
        return splock.tryLock();
    }

    inline void exclusiveUnlock() {
        splock.unlock();
    }

    inline void sharedLock(const int tid) {
        while (true) {
            ri.arrive(tid);
            if (!splock.isLocked()) break;
            ri.depart(tid);
            while (splock.isLocked()) Pause();
        }
    }

    inline void sharedUnlock(const int tid) {
        ri.depart(tid);
    }

    inline void waitForReaders(){
        while (!ri.isEmpty()) {} // spin waiting for readers
    }
};


// The persistent metadata is a 'header' that contains all the logs.
// It is located after back, in the persistent region.
// We hard-code the location of the pwset, so make sure it's the FIRST thing in PMetadata.
struct PMetadata {
    static const uint64_t   MAGIC_ID = 0x1337bab9;
    persist<void*>          rootPtrs[MAX_ROOT_POINTERS];
    uint64_t                id {0};
    volatile uint64_t       state {0};
    uint64_t                padding[8-1];
};


// Address of Persistent Metadata
static PMetadata* const pmd = (PMetadata*)PREGION_ADDR;


// Volatile log
struct AppendLog {
    static const uint64_t CHUNK_SIZE = 16*1024*1024ULL;
    uint64_t       numEntries {0};
    uint8_t**      addr;
    AppendLog*     next {nullptr};

    AppendLog() {
        addr = new uint8_t*[CHUNK_SIZE];
    }

    ~AppendLog() {
        delete[] addr;
    }

    inline void clear() {
        numEntries = 0;
    }

    inline void flushStores(uint64_t offset) {
        for (int i = 0; i < numEntries; i++) {PWB(addr[i] + offset);}
    }

    inline void applyStores(uint64_t offset) {
        for (int i = 0; i < numEntries; i++) {
            *(uint64_t*)(addr[i] + offset) = *(uint64_t*)addr[i];
        }
    }
};


// Counter of nested write transactions
extern thread_local int64_t tl_nested_write_trans;
// Counter of nested read-only transactions
extern thread_local int64_t tl_nested_read_trans;

class RomLog;
extern RomLog gRomLog;


class RomLog {
private:
    // Possible values for "state"
    static const int IDLE = 0;
    static const int MUTATING = 1;
    static const int COPYING = 2;
    // Padding on x86 should be on 2 cache lines
    static const int                       CLPAD = 128/sizeof(uintptr_t);
    bool                                   reuseRegion {false};                 // used by the constructor and initialization
    int                                    pfd {-1};
    CRWWPSpinLock                          rwlock {};
    // Array of atomic pointers to functions (used by Flat Combining)
    alignas(128) std::atomic< std::function<void()>* >* fc;
    alignas(128) EsLoco<persist>                        esloco {};

public:
    struct tmbase : public romlogfc::tmbase { };
    alignas(128) AppendLog                              appendLog {};

    RomLog() {
        fc = new std::atomic< std::function<void()>* >[REGISTRY_MAX_THREADS*CLPAD];
        for (int i = 0; i < REGISTRY_MAX_THREADS; i++) {
            fc[i*CLPAD].store(nullptr, std::memory_order_relaxed);
        }
        mapPersistentRegion(PM_FILE_NAME, PREGION_ADDR, PM_REGION_SIZE);
    }

    ~RomLog() {
        delete[] fc;
    }

    static std::string className() { return "RomLog-FC"; }

    void mapPersistentRegion(const char* filename, uint8_t* regionAddr, const uint64_t regionSize) {
        // Check that the header with the logs leaves at least half the memory available to the user
        if (sizeof(PMetadata) > regionSize/2) {
            printf("ERROR: the size of the logs in persistent memory is so large that it takes more than half the whole persistent memory\n");
            printf("Please reduce some of the settings in TrinityFC.hpp and try again\n");
            assert(false);
        }
        // Check if the file already exists or not
        struct stat buf;
        if (stat(filename, &buf) == 0) {
            // File exists
            pfd = open(filename, O_RDWR|O_CREAT, 0755);
            assert(pfd >= 0);
            reuseRegion = true;
        } else {
            // File doesn't exist
            pfd = open(filename, O_RDWR|O_CREAT, 0755);
            assert(pfd >= 0);
            if (lseek(pfd, regionSize-1, SEEK_SET) == -1) {
                perror("lseek() error");
            }
            if (write(pfd, "", 1) == -1) {
                perror("write() error");
            }
        }
        // Try one time to mmap() with DAX. If it fails due to MAP_FAILED, then retry without DAX.
        // We may fail because the address is not available. Retry at most 4 times, then give up.
        // uint64_t dax_flag = MAP_SYNC;
        void* got_addr = NULL;
        for (int i = 0; i < 4; i++) {
            // got_addr = mmap(regionAddr, regionSize, (PROT_READ | PROT_WRITE), MAP_SHARED_VALIDATE | dax_flag, pfd, 0);
            got_addr = mmap(regionAddr, regionSize, (PROT_READ | PROT_WRITE), MAP_SHARED, pfd, 0);
            if (got_addr == regionAddr) {
            // if (got_addr == NULL) {
                // if (dax_flag == 0) printf("WARNING: running without DAX enabled\n");
                break;
            }
            if (got_addr == MAP_FAILED) {
                // Failed to mmap(). Let's try without DAX.
                // dax_flag = 0;
            } else {
                // mmap() worked but in wrong address. Let's unmap() and try again.
                munmap(got_addr, regionSize);
                // Sleep for a second before retrying the mmap()
                usleep(1000);
            }
        }
        if (got_addr == MAP_FAILED || got_addr != regionAddr) {
        // if (got_addr == MAP_FAILED || got_addr != NULL) {
            printf("got_addr = %p  %p\n", got_addr, MAP_FAILED);
            perror("ERROR: mmap() is not working !!! ");
            assert(false);
        }
        // If the file has just been created or if the header is not consistent, clear everything.
        // Otherwise, re-use and recover to a consistent state.
        if (reuseRegion) {
            recover();
            esloco.init(regionAddr+sizeof(PMetadata), regionSize-sizeof(PMetadata), false);
        } else {
            new (regionAddr) PMetadata();
            esloco.init(regionAddr+sizeof(PMetadata), regionSize-sizeof(PMetadata), true);
            PFENCE();
            pmd->id = PMetadata::MAGIC_ID;
            PWB(&pmd->id);
            PFENCE();
        }
    }

    /* Start a single-threaded durable transaction (pure undo-log, no flat-combining) */
    inline void beginTx() {
        tl_nested_write_trans++;
        if (tl_nested_write_trans > 1) return;
        pmd->state = MUTATING;

        uint64_t* ptr = (uint64_t*)(& pmd->state);
        PWB(ptr);
        // PWB(&pmd->state);
        PFENCE();
    }

    /* End a single-threaded durable transaction */
    inline void endTx() {
        tl_nested_write_trans--;
        if (tl_nested_write_trans > 0) return;
        appendLog.flushStores(0);
        PFENCE();
        pmd->state = COPYING;
        uint64_t* ptr = (uint64_t*)(& pmd->state);
        PWB(ptr);
        // PWB(&pmd->state);
        PSYNC();
        appendLog.applyStores(PBACK_ADDR-PMAIN_ADDR);
        appendLog.flushStores(PBACK_ADDR-PMAIN_ADDR);
        PFENCE();
        appendLog.clear();
        pmd->state = IDLE;
        // No need to pwb() for IDLE state
    }

    void recover() {
        if (pmd->state == COPYING) {
            std::memcpy(PBACK_ADDR, PMAIN_ADDR, PM_REGION_SIZE/2);
            flushFromTo(PBACK_ADDR, PREGION_END);
        } else if (pmd->state == MUTATING) {
            std::memcpy(PMAIN_ADDR + sizeof(PMetadata), PBACK_ADDR + sizeof(PMetadata), (PM_REGION_SIZE/2) - sizeof(PMetadata));
            flushFromTo(PMAIN_ADDR + sizeof(PMetadata), PBACK_ADDR);
        } else {
            return;
        }
        PFENCE();
        pmd->state = IDLE;
        uint64_t* ptr = (uint64_t*)(& pmd->state);
        PWB(ptr);
        // PWB(&pmd->state);
        PSYNC();
    }

    /*
     * Non static, thread-safe
     * Progress: Blocking (starvation-free)
     */
    template<class F> void ns_write_transaction(F&& mutativeFunc) {
        if (tl_nested_write_trans > 0) {
            mutativeFunc();
            return;
        }
        std::function<void()> myfunc = mutativeFunc;
        const int tid = ThreadRegistry::getTID();
        // Add our mutation to the array of flat combining
        fc[tid*CLPAD].store(&myfunc, std::memory_order_release);
        // Lock writersMutex
        while (true) {
            if (rwlock.tryExclusiveLock()) break;
            // Check if another thread executed my mutation
            if (fc[tid*CLPAD].load(std::memory_order_acquire) == nullptr) return;
            std::this_thread::yield();
        }
        bool somethingToDo = false;
        const int maxTid = ThreadRegistry::getMaxThreads();
        // Save a local copy of the flat combining array
        std::function<void()>* lfc[maxTid];
        for (int i = 0; i < maxTid; i++) {
            lfc[i] = fc[i*CLPAD].load(std::memory_order_acquire);
            if (lfc[i] != nullptr) somethingToDo = true;
        }
        // Check if there is at least one operation to apply
        if (!somethingToDo) {
            rwlock.exclusiveUnlock();
            return;
        }
        rwlock.waitForReaders();
        beginTx();
        // Apply all mutativeFunc
        for (int i = 0; i < maxTid; i++) {
            if (lfc[i] == nullptr) continue;
            (*lfc[i])();
        }
        endTx();
        // Inform the other threads their transactions are committed/durable
        for (int i = 0; i < maxTid; i++) {
            if (lfc[i] == nullptr) continue;
            fc[i*CLPAD].store(nullptr, std::memory_order_release);
        }
        // Release the lock
        rwlock.exclusiveUnlock();
    }

    // Non-static thread-safe read-only transaction
    template<class F> void ns_read_transaction(F&& readFunc) {
        if (tl_nested_read_trans > 0) {
            readFunc();
            return;
        }
        int tid = ThreadRegistry::getTID();
        ++tl_nested_read_trans;
        rwlock.sharedLock(tid);
        readFunc();
        rwlock.sharedUnlock(tid);
        --tl_nested_read_trans;
    }

    // It's silly that these have to be static, but we need them for the (SPS) benchmarks due to templatization
    template<typename F> static void updateTx(F&& func) { gRomLog.ns_write_transaction(func); }
    template<typename F> static void readTx(F&& func) { gRomLog.ns_read_transaction(func); }
    // Sequential durable transactions
    template<typename F> static void updateTxSeq(F&& func) { gRomLog.beginTx(); func(); gRomLog.endTx(); }
    template<typename F> static void readTxSeq(F&& func) { func(); }

    // TODO: Remove these two once we make CX have void transactions
    template<typename R,class F>
    inline static R readTx(F&& func) {
        gRomLog.ns_read_transaction([&]() {func();});
        return R{};
    }
    template<typename R,class F>
    inline static R updateTx(F&& func) {
        gRomLog.ns_write_transaction([&]() {func();});
        return R{};
    }

    template <typename T, typename... Args> static T* tmNew(Args&&... args) {
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not allocate outside a transaction\n");
            return nullptr;
        }
        T* ptr = (T*)gRomLog.esloco.malloc(sizeof(T));
        // If we get nullptr then we've ran out of PM space
        assert(ptr != nullptr);
        new (ptr) T(std::forward<Args>(args)...);
        return ptr;
    }

    template<typename T> static void tmDelete(T* obj) {
        if (obj == nullptr) return;
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not de-allocate outside a transaction\n");
            return;
        }
        obj->~T(); // Execute destructor as part of the current transaction
        tmFree(obj);
    }

    static void* tmMalloc(size_t size) {
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not allocate outside a transaction\n");
            return nullptr;
        }
        void* obj = gRomLog.esloco.malloc(size);
        return obj;
    }

    static void tmFree(void* obj) {
        if (obj == nullptr) return;
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not de-allocate outside a transaction\n");
            return;
        }
        gRomLog.esloco.free(obj);
    }

    // TODO: change this to ptmMalloc()
    static void* pmalloc(size_t size) {
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not allocate outside a transaction\n");
            return nullptr;
        }
        return gRomLog.esloco.malloc(size);
    }

    // TODO: change this to ptmFree()
    static void pfree(void* obj) {
        if (obj == nullptr) return;
        if (tl_nested_write_trans == 0) {
            printf("ERROR: Can not de-allocate outside a transaction\n");
            return;
        }
        gRomLog.esloco.free(obj);
    }

    static inline void* get_object(int idx) {
        return pmd->rootPtrs[idx].pload();
    }

    static inline void put_object(int idx, void* obj) {
        pmd->rootPtrs[idx].pstore(obj);
    }
};


// Store interposing
template<typename T> inline void persist<T>::pstore(T newVal) {
    uint8_t* valaddr = (uint8_t*)this;
    if (valaddr >= PREGION_ADDR && valaddr < PREGION_END) {
        assert(gRomLog.appendLog.numEntries != AppendLog::CHUNK_SIZE);
        gRomLog.appendLog.addr[gRomLog.appendLog.numEntries++] = valaddr;
    }
    val = (uint64_t)newVal;
}


//
// Wrapper methods to the global TM instance. The user should use these:
//
template<typename R, typename F> static R updateTx(F&& func) { return gRomLog.updateTx<R>(func); }
template<typename R, typename F> static R readTx(F&& func) { return gRomLog.readTx<R>(func); }
template<typename F> static void updateTx(F&& func) { gRomLog.updateTx(func); }
template<typename F> static void readTx(F&& func) { gRomLog.readTx(func); }
template<typename F> static void updateTxSeq(F&& func) { gRomLog.beginTx(); func(); gRomLog.endTx(); }
template<typename F> static void readTxSeq(F&& func) { func(); }
template<typename T, typename... Args> T* tmNew(Args&&... args) { return RomLog::tmNew<T>(std::forward<Args>(args)...); }
template<typename T> void tmDelete(T* obj) { RomLog::tmDelete<T>(obj); }
inline static void* get_object(int idx) { return RomLog::get_object(idx); }
inline static void put_object(int idx, void* obj) { RomLog::put_object(idx, obj); }
inline static void* tmMalloc(size_t size) { return RomLog::tmMalloc(size); }
inline static void tmFree(void* obj) { RomLog::tmFree(obj); }


//
// Place these in a .cpp if you include this header from multiple files (compilation units)
//
// Global/singleton to hold all the thread registry functionality
ThreadRegistry gThreadRegistry {};
// PTM singleton
RomLog gRomLog {};
// Counter of nested write transactions
thread_local int64_t tl_nested_write_trans {0};
// Counter of nested read-only transactions
thread_local int64_t tl_nested_read_trans {0};
// This is where every thread stores the tid it has been assigned when it calls getTID() for the first time.
// When the thread dies, the destructor of ThreadCheckInCheckOut will be called and de-register the thread.
thread_local ThreadCheckInCheckOut tl_tcico {};
// Helper function for thread de-registration
void thread_registry_deregister_thread(const int tid) {
    gThreadRegistry.deregister_thread(tid);
}

}
#endif /* _ROMULUS_LOG_FLAT_COMBINING_H_ */
