/*
 * dfc.cpp -- Detectable Flat Combining implemented using libpmemobj C++ bindings
 */
#include <sys/stat.h>
#include <bits/stdc++.h>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmem.h>
#include <cmath> 

using namespace pmem;
using namespace pmem::obj;
using namespace std::chrono;
using namespace std::literals::chrono_literals;

#ifdef SAME100_BENCH
#define DATA_FILE "../data/same100-green-pstack-ll-dfc.txt"
#endif

#ifndef DATA_FILE
#define DATA_FILE "../data/green-pstack-ll-dfc.txt"
#endif
#ifndef PDATA_FILE
#define PDATA_FILE "../data/pwb-pfence-dfc.txt"
#endif
#ifndef PM_REGION_SIZE
#define PM_REGION_SIZE 1024*1024*1024ULL // 1GB for now
// #define PM_REGION_SIZE 1024*1024*128ULL 
#endif

// Name of persistent file mapping
#ifndef PM_FILE_NAME
// #define PM_FILE_NAME   "/home/matanr/recov_flat_combining/poolfile"
// #define PM_FILE_NAME   "/dev/shm/dfc_shared"
// #define PM_FILE_NAME   "/dev/dax4.0"
#define PM_FILE_NAME   "/mnt/dfcpmem/dfc_shared"
#endif

// #define N 8  // number of processes
#define N 80  // number of processes

#define MAX_POOL_SIZE 4000  // number of nodes in the pool
// #define MAX_POOL_SIZE 80  // number of nodes in the pool
#define ACK -1
#define EMPTY -2
#define NONE -3
#define PUSH_OP 1
#define POP_OP 0

#define VALID_ANN(dfc, i)   dfc->announce_arr[i]->announces[dfc->announce_arr[i]->valid % 10]
#define ANN(dfc, i, valid)   dfc->announce_arr[i]->announces[valid % 10]


int NN = N;  // number of processes running now
const int num_words = MAX_POOL_SIZE / 64 + 1;
uint64_t free_nodes_log [num_words];

uint64_t free_nodes_log_h1;


// Macros needed for persistence
#ifdef PWB_IS_CLFLUSH_PFENCE_NOP
  /*
   * More info at http://elixir.free-electrons.com/linux/latest/source/arch/x86/include/asm/special_insns.h#L213
   * Intel programming manual at https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
   * Use these for Broadwell CPUs (cervino server)
   */
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")                      // Broadwell only works with this.
  #define PFENCE()               {}                                                                             // No ordering fences needed for CLFLUSH (section 7.4.6 of Intel manual)
  #define PSYNC()                {}  
  #define PPWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")  // parallel PWB
  #define PPFENCE()               {} // parallel PFENCE
#elif PWB_IS_CLFLUSH
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
  #define PPWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory") // parallel PWB
  #define PPFENCE()               __asm__ volatile("sfence" : : : "memory") // parallel PFENCE
#elif PWB_IS_CLWB
  /* Use this for CPUs that support clwb, such as the SkyLake SP series (c5 compute intensive instances in AWS are an example of it) */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))  // clwb() only for Ice Lake onwards
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
  #define PPWB(addr)              __asm__ volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr))) // parallel PWB
  #define PPFENCE()               __asm__ volatile("sfence" : : : "memory") // parallel PFENCE
#elif PWB_IS_NOP
  /* pwbs are not needed for shared memory persistency (i.e. persistency across process failure) */
  #define PWB(addr)              {}
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
  #define PPWB(addr)              {} // parallel PWB
  #define PPFENCE()               __asm__ volatile("sfence" : : : "memory") // parallel PFENCE
#elif PWB_IS_CLFLUSHOPT
  /* Use this for CPUs that support clflushopt, which is most recent x86 */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))    // clflushopt (Kaby Lake)
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
  #define PPWB(addr)             __asm__ volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr))) // parallel PWB
  #define PPFENCE()              __asm__ volatile("sfence" : : : "memory") // parallel PFENCE
#elif PWB_IS_PMEM
  #define PWB(addr)              pmem_flush(addr, sizeof(addr))
  #define PFENCE()               pmem_drain()
  #define PSYNC() 				 {}
  #define PPWB(addr)              pmem_flush(addr, sizeof(addr)) // parallel PWB
  #define PPFENCE()               pmem_drain() // parallel PFENCE
#elif COUNT_PWB
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory") ; localPwbCounter++
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory") ; localPfenceCounter++
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
  #define PPWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory") ; localParallelPwbCounter++
  #define PPFENCE()               __asm__ volatile("sfence" : : : "memory") ; localParallelPfenceCounter++
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSHOPT if you don't know what your CPU is capable of"
#endif


std::atomic<bool> cLock {false};    // holds true when locked, holds false when unlocked
std::atomic<int> gRecoveryLock {0}; // holds 1 when locked, holds 0 when unlocked, holds 2 when it was locked once
std::mutex pLock; // Used to add local PWB and PFENCE instructions count to the global variables

thread_local int localPwbCounter = 0;
thread_local int localPfenceCounter = 0;
int pwbCounter = 0;
int pfenceCounter = 0;

thread_local int localParallelPwbCounter = 0;
thread_local int localParallelPfenceCounter = 0;
int pwbParallelCounter = 0;
int pfenceParallelCounter = 0;

// thread_local int l_combining_counter = 0;
// int combining_counter = 0;

int pushList[N];
int popList[N];
short collectedValid[N];

// struct alignas(32) announce { 
struct announce { 
    p<size_t> val;
    p<size_t> epoch;
	p<char> name;
    p<size_t> param; 
} ;

struct alignas(64) transactional_announce { 
    persistent_ptr<announce> announces [2];
	p<short> valid;
} ;

struct node {
    p<size_t> param;
    persistent_ptr<node> next;
    p<uint64_t> index;
} ;

struct detectable_fc {
	p<size_t> cEpoch = 0;
	persistent_ptr<transactional_announce> announce_arr [N];
	persistent_ptr<node> top [2];
	persistent_ptr<node> nodes_pool [MAX_POOL_SIZE];
};

// pool root structure
struct root {
	persistent_ptr<detectable_fc> dfc;
};

size_t try_to_return(persistent_ptr<detectable_fc> dfc, size_t & opEpoch, size_t pid);
size_t try_to_take_lock(persistent_ptr<detectable_fc> dfc, size_t & opEpoch, size_t pid);

void print_state(persistent_ptr<detectable_fc> dfc) {
    size_t opEpoch = dfc->cEpoch;
    if (opEpoch % 2 == 1) {
        opEpoch ++;
    } 
    std::cout << "~~~ Printing state of epoh: " << opEpoch << " ~~~" << std::endl;
    auto current = dfc->top[(opEpoch/2)%2];
	int counter = 0;
    while (current != NULL) {
        std::cout << "Param: " << current->param << std::endl;
        current = current->next;
		counter ++;
    }
}


void transaction_allocations(persistent_ptr<root> proot, pmem::obj::pool<root> pop) {
	transaction::run(pop, [&] {
		// allocation
		proot->dfc = make_persistent<detectable_fc>();
		proot->dfc->top[0] = NULL;
		proot->dfc->top[1] = NULL;
	
		for (int pid=0; pid<N; pid++) {
			proot->dfc->announce_arr[pid] = make_persistent<transactional_announce>();

			proot->dfc->announce_arr[pid]->announces[0] = make_persistent<announce>();
			proot->dfc->announce_arr[pid]->announces[0]->val = NONE;
			proot->dfc->announce_arr[pid]->announces[0]->epoch = NONE;
			proot->dfc->announce_arr[pid]->announces[0]->name = NONE;
			proot->dfc->announce_arr[pid]->announces[0]->param = NONE;

			proot->dfc->announce_arr[pid]->announces[1] = make_persistent<announce>();
			proot->dfc->announce_arr[pid]->announces[1]->val = NONE;
			proot->dfc->announce_arr[pid]->announces[1]->epoch = NONE;
			proot->dfc->announce_arr[pid]->announces[1]->name = NONE;
			proot->dfc->announce_arr[pid]->announces[1]->param = NONE;

			proot->dfc->announce_arr[pid]->valid = 0;	
		}
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			proot->dfc->nodes_pool[i] = make_persistent<node>();
			proot->dfc->nodes_pool[i]->param = NONE;
			proot->dfc->nodes_pool[i]->next = NULL;
			proot->dfc->nodes_pool[i]->index = i;
		} 
		for (int i=0; i < num_words; i++) {
			free_nodes_log[i] = ~0UL;
		}
		free_nodes_log_h1 = ~0UL;
	});
}


void transaction_deallocations(persistent_ptr<root> proot, pmem::obj::pool<root> pop) {
	transaction::run(pop, [&] {
		for (int pid=0; pid<N; pid++) {
			delete_persistent<announce>(proot->dfc->announce_arr[pid]->announces[0]);
			delete_persistent<announce>(proot->dfc->announce_arr[pid]->announces[1]);
			delete_persistent<transactional_announce>(proot->dfc->announce_arr[pid]);
		}
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			delete_persistent<node>(proot->dfc->nodes_pool[i]);
		} 
		for (int i=0; i < num_words; i++) {
			free_nodes_log[i] = ~0UL;
		}
		free_nodes_log_h1 = ~0UL;
		delete_persistent<detectable_fc>(proot->dfc);
	});
}

size_t lock_taken(persistent_ptr<detectable_fc> dfc, size_t & opEpoch, bool combiner, size_t pid)
{
	if (combiner == false) {
		while (dfc->cEpoch <= opEpoch + 1) {
			// std::this_thread::yield(); // without: faster on threads <= cores. with: keeps scaling even after threads > cores
			if (cLock.load(std::memory_order_acquire) == false && dfc->cEpoch <= opEpoch + 1){
                return try_to_take_lock(dfc, opEpoch, pid);
			}
		}
		return try_to_return(dfc, opEpoch, pid);
	}
	return NONE;
}

size_t try_to_take_lock(persistent_ptr<detectable_fc> dfc, size_t & opEpoch, size_t pid)
{
	bool expected = false;
	bool combiner = cLock.compare_exchange_strong(expected, true);
	return lock_taken(dfc, opEpoch, combiner, pid);
}

size_t try_to_return(persistent_ptr<detectable_fc> dfc, size_t & opEpoch, size_t pid)
{
    // size_t val = dfc->announce_arr[pid]->val;
	size_t val = VALID_ANN(dfc, pid)->val;
    if (val == NONE) { 
		opEpoch += 2;
		return try_to_take_lock(dfc, opEpoch, pid);
	}
	else {
		return val;
	}
}

int reduce(persistent_ptr<detectable_fc> dfc) {
	int top_push = -1;
	int top_pop = -1;

	for (size_t i = 0; i < NN; i++) {
		short validOp = dfc->announce_arr[i]->valid;
		if (validOp / 10 == 1) {
			// size_t opEpoch = ANN(dfc, i, validOp)->epoch;
			// size_t opVal = ANN(dfc, i, validOp)->val;
			// if (opEpoch == dfc->cEpoch || opVal == NONE) {
			size_t opVal = ANN(dfc, i, validOp)->val;
			if (opVal == NONE) {
				ANN(dfc, i, validOp)->epoch = dfc->cEpoch;
				// PWB(&ANN(dfc, i, validOp)->epoch);  // needed if there is a chance that epoch will be persisted but val not
				char opName = ANN(dfc, i, validOp)->name;
				if (opName == PUSH_OP) {
					top_push ++;
					pushList[top_push] = i;
					collectedValid[i] = validOp;
				}
				else if (opName == POP_OP) {
					top_pop ++;
					popList[top_pop] = i;
					collectedValid[i] = validOp;
				}
			}
			else{
				collectedValid[i] = NONE;
			}
		}
	}
	// IMPORTANT! make sure that there is no way that a combined op will change valid after it was collected.
	// if there is a way, we must change below the collected op and not the other struct
	while((top_push != -1) || (top_pop != -1)) {
		if ((top_push != -1) && (top_pop != -1)) {
			size_t cPush = pushList[top_push];
			size_t cPop  = popList[top_pop];
			short validOp = collectedValid[cPush];
			ANN(dfc, cPush, validOp)->val = ACK;
			size_t pushParam = ANN(dfc, cPush, validOp)->param;
			ANN(dfc, cPop, collectedValid[cPop])->val = pushParam;

			top_push --;
			top_pop --;
		}
		else if (top_push != -1) {
			return (top_push + 1);
		}
		else if (top_pop != -1){
			return -1 * (top_pop + 1);
		}
	}
	return 0; // empty list
}

void bin(uint64_t n) 
{ 
    if (n > 1UL) 
    bin(n>>1UL);  
    printf("%d", n & 1UL); 
} 


/* Function to get no of set bits in binary 
representation of positive integer n */
unsigned int countSetBits(uint64_t n) 
{ 
    uint64_t count = 0UL; 
    while (n) { 
        count += n & 1UL; 
        n >>= 1UL; 
    } 
    return count; 
}

// garbage collection, updates is_free for all nodes in the pool
void update_free_nodes(persistent_ptr<detectable_fc> dfc, size_t opEpoch) {

	for (int i=0; i<num_words; i++) {
		free_nodes_log[i] = ~0UL;
	}
	free_nodes_log_h1 = ~0UL;
	auto current = dfc->top[(opEpoch/2)%2];
	while (current != NULL) {
		uint64_t i = current->index;
		uint64_t n = free_nodes_log[i/64];
		uint64_t p = i % 64;
		uint64_t b = 0UL;
		uint64_t mask = 1UL << p; 
		free_nodes_log[i/64] = (n & ~mask) | ((b << p) & mask);

		n = free_nodes_log[i/64];
		uint64_t firstSetBit = log2(n & -n); 
		if (firstSetBit >= 64) { // no free bits in this word
			n = free_nodes_log_h1;
			p = i / 64;
			b = 0UL;
			mask = 1UL << p; 
			free_nodes_log_h1 = (n & ~mask) | ((b << p) & mask);
		}
		current = current->next;
	}
}


size_t combine(persistent_ptr<detectable_fc> dfc, size_t opEpoch, pmem::obj::pool<root> pop, size_t pid) {
	// l_combining_counter ++;
	int top_index = reduce(dfc);
	persistent_ptr<node> head = dfc->top[(opEpoch/2)%2];
	if (top_index != 0) {
		if (top_index > 0) { // push
			top_index = top_index - 1;
			do {
				size_t cId = pushList[top_index];

				uint64_t pos = -1;

				uint64_t n = free_nodes_log_h1;
				uint64_t temp_pos_h1 = log2(n & -n); 
				if (temp_pos_h1 >= 64) {
					std::cerr << "No free nodes / Pool size must be at most 4096 nodes." << std::endl;
					exit(-1);
				}
				n = free_nodes_log[temp_pos_h1];
				uint64_t temp_pos = log2(n & -n); 
				pos = temp_pos + temp_pos_h1*64;
				if (temp_pos >= 64 or pos >= MAX_POOL_SIZE) {
					std::cerr << "No free nodes." << std::endl;
					exit(-1);
				}

				auto newNode = dfc->nodes_pool[pos];
				short validOp = collectedValid[cId];
				size_t newParam = ANN(dfc, cId, validOp)->param;
				newNode->param = newParam;
				newNode->next = head;
				
				n = free_nodes_log[pos/64];
				uint64_t p = pos % 64;
				uint64_t b = 0UL;  // set 0 (not free)
				uint64_t mask = 1UL << p; 

				free_nodes_log[pos/64] = (n & ~mask) | ((b << p) & mask);
				n = free_nodes_log[pos/64];
				uint64_t firstSetBit = log2(n & -n); 
				if (firstSetBit >= 64) { // no free bits in this word
					n = free_nodes_log_h1;
					p = pos / 64;
					b = 0UL;
					mask = 1UL << p; 
					free_nodes_log_h1 = (n & ~mask) | ((b << p) & mask);
				}

				ANN(dfc, cId, validOp)->val = ACK;
				// pwbCounter3 ++;
				PWB(&newNode);
				head = newNode;
				top_index -- ;
			} while (top_index != -1);
		}
		else { // pop. should convert to positive index
			top_index = -1 * top_index - 1;
			do {
				size_t cId = popList[top_index];
				if (head == NULL) {
					ANN(dfc, cId, collectedValid[cId])->val = EMPTY;
					// exit(-1);
				}
				else {
                    size_t headParam = head->param;
					ANN(dfc, cId, collectedValid[cId])->val = headParam;

					uint64_t i = head->index;
					uint64_t n = free_nodes_log[i/64];
					uint64_t firstSetBit = log2(n & -n); 
					if (firstSetBit >= 64) { // no free bits in this word
						n = free_nodes_log_h1;
						uint64_t p = i / 64;
						uint64_t b = 1UL;
						uint64_t mask = 1UL << p; 
						free_nodes_log_h1 = (n & ~mask) | ((b << p) & mask);
					}

					n = free_nodes_log[i/64];
					uint64_t p = i % 64;
					uint64_t b = 1UL;  // set 1 (free)
					uint64_t mask = 1UL << p; 

					free_nodes_log[i/64] = (n & ~mask) | ((b << p) & mask);
					
					head = head->next;
				}
				top_index -- ;
			} while (top_index != -1);
		}		
	}
	dfc->top[(opEpoch/2 + 1) % 2] = head;
	for (int i=0; i<NN; i++) { //maybe persist on line. check on optane
		short validOp = collectedValid[i];
		if (validOp != NONE) {
			// pwbCounter5 ++;
			// PWB(&(ANN(dfc, i, validOp)->val));
			// PWB(&(ANN(dfc, i, validOp)->epoch));
			PWB(&ANN(dfc, i, validOp));
			// PWB(&dfc->announce_arr[i]->valid);
			// PWB(&dfc->announce_arr[i]);
		}
	}
	// pwbCounter6 ++;
	PWB(&dfc->top[(opEpoch/2 + 1) % 2]);
	// pfenceCounter3 ++;
	PFENCE();
	dfc->cEpoch = dfc->cEpoch + 1;
	// pwbCounter7 ++;
	// this is important for the following case: the combiner updates the cEpoch, then several ops started to finish and return, 
	// BEFORE cEpoch is persisted. then, when the system recovers we can't distinguish between the following cases: 
	// 1. the combiner finished an operation and updated cEpoch (because it is not persisted), and several ops returned
	// 2. the combiner was in a middle of the combining session (for example).
	PWB(&dfc->cEpoch);
	// pfenceCounter4 ++;
	PFENCE();
	dfc->cEpoch = dfc->cEpoch + 1;
	// pwbCounter8 ++;
	// PWB(&dfc->cEpoch); 
	// pfenceCounter5 ++;
	// PFENCE();
	cLock.store(false, std::memory_order_release);
	size_t value =  try_to_return(dfc, opEpoch, pid);
	return value;
}


size_t op(persistent_ptr<detectable_fc> dfc, pmem::obj::pool<root> pop, size_t pid, char opName, size_t param)
{
	size_t opEpoch = dfc->cEpoch;
	if (opEpoch % 2 == 1) {
		opEpoch ++;
	} 
	// announce
	char nextOp = 1 - dfc->announce_arr[pid]->valid % 10;	
	
	ANN(dfc, pid, nextOp)->val = NONE;
	ANN(dfc, pid, nextOp)->epoch = opEpoch; 
	ANN(dfc, pid, nextOp)->param = param;
    ANN(dfc, pid, nextOp)->name = opName;
	
	PPWB(&ANN(dfc, pid, nextOp));
	PPFENCE();
	dfc->announce_arr[pid]->valid = nextOp; // combiner still will not collect it
	PPWB(&dfc->announce_arr[pid]->valid);
	PPFENCE();
	dfc->announce_arr[pid]->valid = 10 + nextOp; // now the combiner can collect
	
	// pwbCounter9[pid] ++;
	// std::atomic_fetch_add(&pwbCounter9, 1);
	// PWB(&dfc->announce_arr[pid]);
	
	// PWB(&dfc->announce_arr[pid]->valid);
	// PFENCE();

	// pfenceCounter6[pid] ++;
	// std::atomic_fetch_add(&pfenceCounter6, 1);
	
	// if after crash we see valid=true, we can be sure that all announcements were completed

	// dfc->announce_arr[pid]->valid = '1';

	size_t value = try_to_take_lock(dfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	opEpoch = dfc->cEpoch;  // this is important for cases in which a late-arriving process eventually gets to be a combiner
	return combine(dfc, opEpoch, pop, pid);
}


// global recovery function, can be executed by the first thread via lock in the individual recovery
// We assume that every thread runs this function right after a (system-wide) crash
size_t recover(persistent_ptr<detectable_fc> dfc, pmem::obj::pool<root> pop, size_t pid, bool opName, size_t param)
{
	int expected = 0;
	bool globalRecovery = gRecoveryLock.compare_exchange_strong(expected, 1);
	if (globalRecovery) {
		// garbage collect and update what nodes are free
		// if (! garbage_collected) {
		update_free_nodes(dfc, dfc->cEpoch);
		// 	garbage_collected = true;
		// } 
		if (dfc->cEpoch%2 == 1) {
			dfc->cEpoch = dfc->cEpoch + 1;
			// pwbCounter1 ++;
			PWB(&dfc->cEpoch);
			// pfenceCounter1 ++;
			PFENCE(); 
		}
		for (int i=0; i<NN; i++) { 
			short validOp = dfc->announce_arr[i]->valid;
			size_t opEpoch = ANN(dfc, i, validOp)->epoch;
			if (validOp / 10 == 0 and opEpoch != NONE) { // if not valid and announced properly - make it valid, i.e. allow the combiner to collect
				dfc->announce_arr[i]->valid = 10 + validOp;
			}
			if (opEpoch == dfc->cEpoch) { 
				ANN(dfc, i, validOp)->val = NONE;
			}
		}
		size_t opEpoch = dfc->cEpoch;
		combine(dfc, opEpoch, pop, pid);
		gRecoveryLock.store(2, std::memory_order_release);
	}
	else {
		while (gRecoveryLock.load() == 1) {} // Spin until recovery is complete
	}
	// if (VALID_ANN(dfc, pid)->epoch == NONE) {
	// 	// did not announce properly
	// 	return op(dfc, pop, pid, opName, param);
	// }
	if (VALID_ANN(dfc, pid)->name == NONE) {
		// did not announce properly
		return op(dfc, pop, pid, opName, param);
	}
	return VALID_ANN(dfc, pid)->val;
	
	// if ((int) dfc->announce_arr[pid]->valid / 10 < 0) { 
	// 	// did not announce properly
	// 	return op(dfc, pop, pid, opName, param);
	// }

	// size_t opEpoch = VALID_ANN(dfc, pid)->epoch;
    // size_t opVal = VALID_ANN(dfc, pid)->val;
	// if (opVal != NONE and dfc->cEpoch >= opEpoch + 1) {
	// 	return opVal;
	// }
	// // char validOp = dfc->announce_arr[pid]->valid;
	// // if ((int) validOp / 10 == 0) { // if not valid - make it valid, i.e. allow the combiner to collect
	// // 	dfc->announce_arr[pid]->valid = 10 + (int) validOp;
	// // }
    // size_t value = try_to_take_lock(dfc, opEpoch, pid);
	// if (value != NONE){
	// 	return value;
	// }
	// opEpoch = dfc->cEpoch;  // this is important for cases in which a late-arriving process eventually gets to be a combiner
	
	// return combine(dfc, opEpoch, pop, pid);
}


inline bool is_file_exists (const char* name) {
  struct stat buffer;   
  return (stat (name, &buffer) == 0); 
}

/**
 * enqueue-dequeue pairs: in each iteration a thread executes an enqueue followed by a dequeue;
 * the benchmark executes 10^8 pairs partitioned evenly among all threads;
 */
std::tuple<uint64_t, double, double, double, double> pushPopTest(int numThreads, const long numPairs, const int numRuns, const int numSameOps) {
	const uint64_t kNumElements = 0; // Number of initial items in the stack
	static const long long NSEC_IN_SEC = 1000000000LL;
	
	pmem::obj::pool<root> pop;
	pmem::obj::persistent_ptr<root> proot;

	const char* pool_file_name = PM_FILE_NAME;

	// pop = pool<root>::create(pool_file_name, "layout", (size_t)PM_REGION_SIZE, S_IRUSR|S_IWUSR);
	// proot = pop.root();
	// transaction_allocations(proot, pop);
	// std::cout << "Finished allocating!" << std::endl;

    size_t params [N];
    size_t ops [N];
    std::thread threads_pool[N];

	std::cout << "in push pop" << std::endl;
	nanoseconds deltas[numThreads][numRuns];
	std::atomic<bool> startFlag = { false };

	std::cout << "##### " << "Detectable Flat Combining" << " #####  \n";

	auto pushpop_lambda = [&numThreads, &startFlag,&numPairs, &proot, &pop](nanoseconds *delta, const int tid) {
		//UserData* ud = new UserData{0,0};
		size_t param = tid;
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < numPairs/numThreads; iter++) {
			op(proot->dfc, pop, tid, PUSH_OP, param);
			if (op(proot->dfc, pop, tid, POP_OP, NONE) == EMPTY) std::cout << "Error at measurement pop() iter=" << iter << "\n";
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
		std::lock_guard<std::mutex> lock(pLock);
		pwbCounter += localPwbCounter;
		pfenceCounter += localPfenceCounter;
		pwbParallelCounter += localParallelPwbCounter;
		pfenceParallelCounter += localParallelPfenceCounter;
		// combining_counter += l_combining_counter;
	};

	auto pushpop_k_lambda = [&numThreads, &startFlag,&numPairs, &numSameOps, &proot, &pop](nanoseconds *delta, const int tid) {
		//UserData* ud = new UserData{0,0};
		size_t param = tid;
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < numPairs/(numThreads*numSameOps); iter++) {
			for (long iter_s = 0; iter_s < numSameOps; iter++) {
				op(proot->dfc, pop, tid, PUSH_OP, param);
			}
			for (long iter_s = 0; iter_s < numSameOps; iter++) {
				if (op(proot->dfc, pop, tid, POP_OP, NONE) == EMPTY) std::cout << "Error at measurement pop() iter=" << iter << "\n";
			}
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
		std::lock_guard<std::mutex> lock(pLock);
		pwbCounter += localPwbCounter;
		pfenceCounter += localPfenceCounter;
		pwbParallelCounter += localParallelPwbCounter;
		pfenceParallelCounter += localParallelPfenceCounter;
		// combining_counter += l_combining_counter;
	};

	auto randop_lambda = [&numThreads, &startFlag,&numPairs, &proot, &pop](nanoseconds *delta, const int tid) {
		size_t param = tid;
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		// thread_local int operations[2 * numPairs/numThreads];
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < 2 * numPairs/numThreads; iter++) {
			int randop = rand() % 2;         // randop in the range 0 to 1
			if (randop == 0) {
				op(proot->dfc, pop, tid, PUSH_OP, param);
			}
			else if (randop == 1) {
				op(proot->dfc, pop, tid, POP_OP, NONE);
			}
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
		std::lock_guard<std::mutex> lock(pLock);
		pwbCounter += localPwbCounter;
		pfenceCounter += localPfenceCounter;
		pwbParallelCounter += localParallelPwbCounter;
		pfenceParallelCounter += localParallelPfenceCounter;
		// combining_counter += l_combining_counter;
	};

	for (int irun = 0; irun < numRuns; irun++) {
		NN = numThreads;
		
		pop = pool<root>::create(pool_file_name, "layout", (size_t)PM_REGION_SIZE, S_IRUSR|S_IWUSR);
		proot = pop.root();
		transaction_allocations(proot, pop);
		std::cout << "Finished allocating!" << std::endl;

		// Fill the queue with an initial amount of nodes
		size_t param = size_t(41);
		for (uint64_t ielem = 0; ielem < kNumElements; ielem++) {
			op(proot->dfc, pop, 0, PUSH_OP, param);
		}
		std::thread enqdeqThreads[numThreads];
		#ifdef SAME100_BENCH
		// for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(randop_lambda, &deltas[tid][irun], tid);
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(pushpop_k_lambda, &deltas[tid][irun], tid);
		#else
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(pushpop_lambda, &deltas[tid][irun], tid);
		#endif
		startFlag.store(true);
		// Sleep for 2 seconds just to let the threads see the startFlag
		std::this_thread::sleep_for(2s);
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid].join();
		startFlag.store(false);

		transaction_deallocations(proot, pop);
		/* Cleanup */
		/* Close persistent pool */
		pop.close ();	
		std::remove(pool_file_name);
	}

	// Sum up all the time deltas of all threads so we can find the median run
	std::vector<nanoseconds> agg(numRuns);
	for (int irun = 0; irun < numRuns; irun++) {
		agg[irun] = 0ns;
		for (int tid = 0; tid < numThreads; tid++) {
			agg[irun] += deltas[tid][irun];
		}
	}

	// Compute the median. numRuns should be an odd number
	sort(agg.begin(),agg.end());
	auto median = agg[numRuns/2].count()/numThreads; // Normalize back to per-thread time (mean of time for this run)

	std::cout << "Total Ops/sec = " << numPairs*2*NSEC_IN_SEC/median << "\n";
	// std::cout << "combining_counter: " << combining_counter << std::endl;
	// combining_counter = 0;
	// l_combining_counter = 0;
	#if defined(COUNT_PWB)
		double pwbPerOp = double(pwbCounter) / double(numPairs*2);
		double pfencePerOp = double(pfenceCounter) / double(numPairs*2);
		double pwbParallelPerOp = double(pwbParallelCounter) / double(numPairs*2);
		double pfenceParallelPerOp = double(pfenceParallelCounter) / double(numPairs*2);
		std::cout << "#pwb/#op: " << std::fixed << pwbPerOp;
		std::cout << ", #pfence/#op: " << std::fixed << pfencePerOp;
		std::cout << ", T #pwb/#op: " << std::fixed << pwbPerOp + pwbParallelPerOp;
		std::cout << ", T #pfence/#op: " << std::fixed << pfencePerOp + pfenceParallelPerOp << std::endl;
		// std::cout << ", Total #pwb/#op (parallel PWBs included): " << std::fixed << pwbPerOp + pwbParallelPerOp;
		// std::cout << "#Total pfence/#op (parallel PFENCEs included): " << std::fixed << pfencePerOp + pfenceParallelPerOp << std::endl;
		
		pwbCounter = 0; pfenceCounter = 0; pwbParallelCounter = 0; pfenceParallelCounter = 0;
		localPwbCounter = 0; localPfenceCounter = 0; localParallelPwbCounter = 0; localParallelPfenceCounter = 0;
        return std::make_tuple(numPairs*2*NSEC_IN_SEC/median, pwbPerOp, pfencePerOp, pwbPerOp + pwbParallelPerOp, pfencePerOp + pfenceParallelPerOp);
	#endif
	return std::make_tuple(numPairs*2*NSEC_IN_SEC/median, 0, 0, 0, 0);
}


#define MILLION  1000000LL

int runSeveralTests() {
    const std::string dataFilename { DATA_FILE };
	const std::string pdataFilename { PDATA_FILE };
	std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 24, 32, 40 };     // For Castor
    // std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40};     // For Castor
	// std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 64, 68, 72, 76, 80 };     // For Castor
    const int numRuns = 10;                                           // Number of runs
    const long numPairs = 1*MILLION;                                 // 1M is fast enough on the laptop
	const int numSameOps = 100;

    std::tuple<uint64_t, double, double, double, double> results[threadList.size()];
    std::string cName = "DFC";
    // Reset results
    std::memset(results, 0, sizeof(uint64_t)*threadList.size());

    // Enq-Deq Throughput benchmarks
    for (int it = 0; it < threadList.size(); it++) {
        int nThreads = threadList[it];
        std::cout << "\n----- pstack-ll (push-pop)   threads=" << nThreads << "   pairs=" << numPairs/MILLION << "M   runs=" << numRuns << " -----\n";
		results[it] = pushPopTest(nThreads, numPairs, numRuns, numSameOps);
    }

	#if not defined(COUNT_PWB)
    // Export tab-separated values to a file to be imported in gnuplot or excel
    std::ofstream dataFile;
    dataFile.open(dataFilename);
    dataFile << "Threads\t";
    // Printf class names for each column
    dataFile << cName << "\t";
    dataFile << "\n";
    for (int it = 0; it < threadList.size(); it++) {
        dataFile << threadList[it] << "\t";
        dataFile << std::get<0>(results[it]) << "\t";
        dataFile << "\n";
    }
    dataFile.close();
    std::cout << "\nSuccessfuly saved results in " << dataFilename << "\n";
	#endif

	#if defined(COUNT_PWB)
    // Export tab-separated values to a file to be imported in gnuplot or excel
    std::ofstream pdataFile;
    pdataFile.open(pdataFilename);
    pdataFile << "Threads\t";
    // Printf class names for each column
    pdataFile << "DFC-PWB" << "\t" << "DFC-PFENCE" << "\t" << "DFC-PWB-T" << "\t" << "DFC-PFENCE-T" << "\t";
    pdataFile << "\n";
    for (int it = 0; it < threadList.size(); it++) {
        pdataFile << threadList[it] << "\t";
        pdataFile << std::get<1>(results[it]) << "\t";
        pdataFile << std::get<2>(results[it]) << "\t";
		pdataFile << std::get<3>(results[it]) << "\t";
        pdataFile << std::get<4>(results[it]) << "\t";
        pdataFile << "\n";
    }
    pdataFile.close();
    std::cout << "\nSuccessfuly saved results in " << pdataFilename << "\n";
    #endif

    return 0;
}


int recoveryTest() {
	NN = 8;
	pmem::obj::pool<root> pop;
	pmem::obj::persistent_ptr<root> proot;

	// const char* pool_file_name = "poolfile";
	const char* pool_file_name = PM_FILE_NAME;
    size_t params [NN];
    size_t ops [NN];
    std::thread threads_pool[NN];

    for (int pid=0; pid<NN; pid++) {
		if (pid % 3 == 1) {
			params[pid] = NONE;
			ops[pid] = POP_OP;
			std::cout << "pop, ";
		}
		else {
			params[pid] = pid;
			ops[pid] = PUSH_OP;
			std::cout << "push, ";
		}
	}
	std::cout << std::endl;

	if (is_file_exists(pool_file_name)) {
		// open a pmemobj pool
		pop = pool<root>::open(pool_file_name, "layout");
		proot = pop.root();

		std::cout << "printing before recovering" << std::endl;
		print_state(proot->dfc);
		
        for (int pid=0; pid<NN; pid++) {
            threads_pool[pid] = std::thread (recover, proot->dfc, pop, pid, ops[pid], params[pid]);
        }							      
		for (int pid=0; pid<NN; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->dfc);
		std::cout << "finished printing after recovering" << std::endl;
		
		
		for (int pid=0; pid<NN; pid++) {
			char nextOp = 1 - proot->dfc->announce_arr[pid]->valid % 10;	
			ANN(proot->dfc, pid, nextOp)->epoch = NONE; // change the last field: 
            threads_pool[pid] = std::thread (op, proot->dfc, pop, pid, ops[pid], params[pid]);
        }							      
		for (int pid=0; pid<NN; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->dfc);

		transaction_deallocations(proot, pop);
		/* Cleanup */
		/* Close persistent pool */
		pop.close ();	
		std::remove(pool_file_name);
		return 1;
	}
	else {
		// create a pmemobj pool
		// pop = pool<root>::create(pool_file_name, "layout", PMEMOBJ_MIN_POOL);
		pop = pool<root>::create(pool_file_name, "layout", PM_REGION_SIZE);
		proot = pop.root();
		transaction_allocations(proot, pop);
		std::cout << "Finished allocating!" << std::endl;
		
		for (int pid=0; pid<NN; pid++) {
			char nextOp = 1 - proot->dfc->announce_arr[pid]->valid % 10;	
			// ANN(proot->dfc, pid, nextOp)->epoch = NONE; 
			ANN(proot->dfc, pid, nextOp)->name = NONE; 
            threads_pool[pid] = std::thread (op, proot->dfc, pop, pid, ops[pid], params[pid]);
        }
		// usleep(1);
		kill(getpid(), SIGKILL);
		for (int pid=0; pid<NN; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->dfc);
		return 0;
	}
}


int main(int argc, char *argv[]) {
	
	// recoveryTest();
	runSeveralTests();
}
