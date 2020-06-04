/*
 * recov_fc.cpp -- recoverable flat combining implemented using libpmemobj C++ bindings
 */
#include <sys/stat.h>
#include <bits/stdc++.h>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <unistd.h>
#include <thread>
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

#ifndef DATA_FILE
#define DATA_FILE "data/green-pstack-ll-rfc.txt"
#endif
#ifndef PM_REGION_SIZE
#define PM_REGION_SIZE 1024*1024*1024ULL // 1GB for now
// #define PM_REGION_SIZE 1024*1024*128ULL 
#endif
// Name of persistent file mapping
#ifndef PM_FILE_NAME
// #define PM_FILE_NAME   "/home/matanr/recov_flat_combining/poolfile"
#define PM_FILE_NAME   "/dev/shm/rfc_shared"
#endif

#define N 60  // number of processes
// #define MAX_POOL_SIZE 4000  // number of nodes in the pool
#define MAX_POOL_SIZE 60  // number of nodes in the pool
#define ACK -1
#define EMPTY -2
#define NONE -3
#define PUSH_OP '1'
#define POP_OP '0'

#define VALID_ANN(rfc, i)   rfc->announce_arr[i]->announces[toDigit(rfc->announce_arr[i]->valid)]
#define ANN(rfc, i, valid)   rfc->announce_arr[i]->announces[toDigit(valid)]

// transforms a char into an int 0-9 digit
#define toDigit(c) (c-'0')

// transforms an int 0-9 digit into a char
#define toChar(i) ('0' + i)


int NN = N;  // number of processes running now
const int num_words = MAX_POOL_SIZE / 64 + 1;
uint64_t free_nodes_log [num_words];

//
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
  #define PSYNC()                {}                                                                             // For durability it's not obvious, but CLFLUSH seems to be enough, and PMDK uses the same approach
#elif PWB_IS_CLFLUSH
  #define PWB(addr)              __asm__ volatile("clflush (%0)" :: "r" (addr) : "memory")
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif PWB_IS_CLWB
  /* Use this for CPUs that support clwb, such as the SkyLake SP series (c5 compute intensive instances in AWS are an example of it) */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))  // clwb() only for Ice Lake onwards
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif PWB_IS_NOP
  /* pwbs are not needed for shared memory persistency (i.e. persistency across process failure) */
  #define PWB(addr)              {}
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif PWB_IS_CLFLUSHOPT
  /* Use this for CPUs that support clflushopt, which is most recent x86 */
  #define PWB(addr)              __asm__ volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))    // clflushopt (Kaby Lake)
  #define PFENCE()               __asm__ volatile("sfence" : : : "memory")
  #define PSYNC()                __asm__ volatile("sfence" : : : "memory")
#elif PWB_IS_PMEM
  #define PWB(addr)              pmem_flush(addr, sizeof(addr))
  #define PFENCE()               pmem_drain()
  #define PSYNC() 				 {}
#else
#error "You must define what PWB is. Choose PWB_IS_CLFLUSHOPT if you don't know what your CPU is capable of"
#endif


std::atomic<bool> cLock {false};    // holds true when locked, holds false when unlocked
bool garbage_collected = false;


int pwbCounter = 0; int pwbCounter1=0; int pwbCounter2=0; int pwbCounter3=0; int pwbCounter4=0; int pwbCounter5=0; int pwbCounter6=0; int pwbCounter7=0; int pwbCounter8=0; int pwbCounter10=0;
int pfenceCounter = 0; int pfenceCounter1=0; int pfenceCounter2=0; int pfenceCounter3=0; int pfenceCounter4=0; int pfenceCounter5=0; int pfenceCounter7=0;
// int pwbCounter9[N];
// int pfenceCounter6[N];
nanoseconds combineCounter = 0ns; nanoseconds reduceCounter = 0ns; nanoseconds findFreeCounter = 0ns; 

int pushList[N];
int popList[N];
char collectedValid[N];
int opsList[N];

struct alignas(32) announce { 
    p<size_t> val;
    p<size_t> epoch;
	p<char> name;
    p<size_t> param; 
} ;

struct alignas(64) transactional_announce { 
    persistent_ptr<announce> announces [2];
	p<char> valid;
} ;

struct node {
    p<size_t> param;
    persistent_ptr<node> next;
    p<uint64_t> index;
} ;

struct recoverable_fc {
	p<size_t> cEpoch = 0;
	persistent_ptr<transactional_announce> announce_arr [N];
	persistent_ptr<node> top [2];
	persistent_ptr<node> nodes_pool [MAX_POOL_SIZE];
};

// pool root structure
struct root {
	persistent_ptr<recoverable_fc> rfc;
};

size_t try_to_return(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, size_t pid);
size_t try_to_take_lock(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, size_t pid);

void print_state(persistent_ptr<recoverable_fc> rfc) {
    size_t opEpoch = rfc->cEpoch;
    if (opEpoch % 2 == 1) {
        opEpoch ++;
    } 
    std::cout << "~~~ Printing state of epoh: " << opEpoch << " ~~~" << std::endl;
    auto current = rfc->top[(opEpoch/2)%2];
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
		proot->rfc = make_persistent<recoverable_fc>();
		proot->rfc->top[0] = NULL;
		proot->rfc->top[1] = NULL;
	
		for (int pid=0; pid<N; pid++) {
			proot->rfc->announce_arr[pid] = make_persistent<transactional_announce>();

			proot->rfc->announce_arr[pid]->announces[0] = make_persistent<announce>();
			proot->rfc->announce_arr[pid]->announces[0]->val = NONE;
			proot->rfc->announce_arr[pid]->announces[0]->epoch = 0;
			proot->rfc->announce_arr[pid]->announces[0]->name = NONE;
			proot->rfc->announce_arr[pid]->announces[0]->param = NONE;

			proot->rfc->announce_arr[pid]->announces[1] = make_persistent<announce>();
			proot->rfc->announce_arr[pid]->announces[1]->val = NONE;
			proot->rfc->announce_arr[pid]->announces[1]->epoch = 0;
			proot->rfc->announce_arr[pid]->announces[1]->name = NONE;
			proot->rfc->announce_arr[pid]->announces[1]->param = NONE;

			proot->rfc->announce_arr[pid]->valid = '0';
			
		}
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			proot->rfc->nodes_pool[i] = make_persistent<node>();
			proot->rfc->nodes_pool[i]->param = NONE;
			proot->rfc->nodes_pool[i]->next = NULL;
			proot->rfc->nodes_pool[i]->index = i;
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
			delete_persistent<announce>(proot->rfc->announce_arr[pid]->announces[0]);
			delete_persistent<announce>(proot->rfc->announce_arr[pid]->announces[1]);
			delete_persistent<transactional_announce>(proot->rfc->announce_arr[pid]);
		}
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			delete_persistent<node>(proot->rfc->nodes_pool[i]);
		} 
		for (int i=0; i < num_words; i++) {
			free_nodes_log[i] = ~0UL;
		}
		free_nodes_log_h1 = ~0UL;
		delete_persistent<recoverable_fc>(proot->rfc);
	});
}

size_t lock_taken(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, bool combiner, size_t pid)
{
	if (combiner == false) {
		while (rfc->cEpoch <= opEpoch + 1) {
			std::this_thread::yield();
			if (cLock.load(std::memory_order_acquire) == false && rfc->cEpoch <= opEpoch + 1){
                return try_to_take_lock(rfc, opEpoch, pid);
			}
		}
		return try_to_return(rfc, opEpoch, pid);
	}
	return NONE;
}

size_t try_to_take_lock(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, size_t pid)
{
	bool expected = false;
	bool combiner = cLock.compare_exchange_strong(expected, true);
	return lock_taken(rfc, opEpoch, combiner, pid);
}

size_t try_to_return(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, size_t pid)
{
    // size_t val = rfc->announce_arr[pid]->val;
	size_t val = VALID_ANN(rfc, pid)->val;
    if (val == NONE) { 
		opEpoch += 2;
		return try_to_take_lock(rfc, opEpoch, pid);
	}
	else {
		return val;
	}
}


// int reduce(persistent_ptr<recoverable_fc> rfc) {
// 	auto startBeats = steady_clock::now();
// 	int top_index = -1;
// 	if (rfc->cEpoch%2 == 1) {
// 		rfc->cEpoch = rfc->cEpoch + 1;
// 		pwbCounter1 ++;
// 		PWB(&rfc->cEpoch);
// 		pfenceCounter1 ++;
// 		PFENCE(); 
// 	}
	
// 	for (size_t i = 0; i < NN; i++) {
// 		if (rfc->announce_arr[i] == NULL) {
// 			continue;
// 		}
// 		bool isOpValid = rfc->announce_arr[i]->valid; // next two lines 
// 		size_t opEpoch = rfc->announce_arr[i]->epoch;
// 		size_t opVal = rfc->announce_arr[i]->val;
// 		if (isOpValid && (opEpoch == rfc->cEpoch || opVal == NONE)) {
// 			rfc->announce_arr[i]->epoch = rfc->cEpoch;

//             bool opName = rfc->announce_arr[i]->name;
//             size_t opParam = rfc->announce_arr[i]->param;
// 			if (opName == PUSH_OP) {
// 				if (top_index == -1) {
// 					top_index ++;
// 					opsList[top_index] = i;
// 				}
// 				else {
// 					size_t cId = opsList[top_index];
// 					bool cOp = rfc->announce_arr[cId]->name;
// 					if (cOp == PUSH_OP) {
// 						top_index ++;
// 						opsList[top_index] = i;
// 					}
// 					else {
// 						rfc->announce_arr[i]->val = ACK;
// 						rfc->announce_arr[cId]->val = opParam;
// 						top_index --;
// 					}
// 				}
// 			}
// 			else if (opName == POP_OP) {
// 				if (top_index == -1) {
// 					top_index ++;
// 					opsList[top_index] = i;
// 				}
// 				else {
// 					size_t cId = opsList[top_index];
// 					bool cOp = rfc->announce_arr[cId]->name;
// 					if (cOp == POP_OP) {
// 						top_index ++;
// 						opsList[top_index] = i;
// 					}
// 					else {
// 						rfc->announce_arr[cId]->val = ACK;
// 						size_t pushParam = rfc->announce_arr[cId]->param;
// 						rfc->announce_arr[i]->val = pushParam;
// 						top_index --;
// 					}
// 				}
// 			}
// 		}
// 	}
// 	auto stopBeats = steady_clock::now();
// 	reduceCounter += stopBeats - startBeats;
// 	return top_index;
// }

int reduce(persistent_ptr<recoverable_fc> rfc) {
	int top_push = -1;
	int top_pop = -1;
	if (rfc->cEpoch%2 == 1) {
		rfc->cEpoch = rfc->cEpoch + 1;
		pwbCounter1 ++;
		PWB(&rfc->cEpoch);
		pfenceCounter1 ++;
		PFENCE(); 
	}

	for (size_t i = 0; i < NN; i++) {
		char validOp = rfc->announce_arr[i]->valid;
		// v, op = valid
		// if (v == 0) {
		size_t opEpoch = ANN(rfc, i, validOp)->epoch;
		size_t opVal = ANN(rfc, i, validOp)->val;
		if (opEpoch == rfc->cEpoch || opVal == NONE) {
			ANN(rfc, i, validOp)->epoch = rfc->cEpoch;
			char opName = ANN(rfc, i, validOp)->name;
			size_t opParam = ANN(rfc, i, validOp)->param;
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

	// IMPORTANT! make sure that there is no way that a combined op will change valid after it was collected.
	// if there is a way, we must change below the collected op and not the other struct
	size_t cPush;
	size_t cPop;
	while((top_push != -1) || (top_pop != -1)) {
		if ((top_push != -1) && (top_pop != -1)) {
			cPush = pushList[top_push];
			cPop  = popList[top_pop];
			char validOp = collectedValid[cPush];
			ANN(rfc, cPush, validOp)->val = ACK;
			size_t pushParam = ANN(rfc, cPush, validOp)->param;
			ANN(rfc, cPop, collectedValid[cPop])->val = pushParam;

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
void update_free_nodes(persistent_ptr<recoverable_fc> rfc, size_t opEpoch) {
	// each node is free, unless current top is somehow points to it
	// for (int i=0; i<MAX_POOL_SIZE; i++) {
	// 	rfc->nodes_pool[i]->is_free = true;
	// }
	// int notfree_count = 0;
	// auto current = rfc->top[(opEpoch/2)%2];
	// while (current != NULL) {
	// 	current->is_free = false;
	// 	current = current->next;
	// 	notfree_count ++;
	// }

	for (int i=0; i<num_words; i++) {
		free_nodes_log[i] = ~0UL;
	}
	free_nodes_log_h1 = ~0UL;
	auto current = rfc->top[(opEpoch/2)%2];
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


size_t combine(persistent_ptr<recoverable_fc> rfc, size_t opEpoch, pmem::obj::pool<root> pop, size_t pid) {
	int top_index = reduce(rfc);
	persistent_ptr<node> head = rfc->top[(opEpoch/2)%2];
	if (top_index != 0) {
		size_t cId;
		if (top_index > 0) { // push
			top_index = top_index - 1;
			do {
				cId = pushList[top_index];

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

				auto newNode = rfc->nodes_pool[pos];
				char validOp = collectedValid[cId];
				size_t newParam = ANN(rfc, cId, validOp)->param;
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

				ANN(rfc, cId, validOp)->val = ACK;
				pwbCounter3 ++;
				PWB(&newNode);
				head = newNode;
				top_index -- ;
			} while (top_index != -1);
		}
		else { // pop. should convert to positive index
			top_index = -1 * top_index - 1;
			size_t cId;
			do {
				cId = popList[top_index];
				if (head == NULL) {
					ANN(rfc, cId, collectedValid[cId])->val = EMPTY;
					// exit(-1);
				}
				else {
                    size_t headParam = head->param;
					ANN(rfc, cId, collectedValid[cId])->val = headParam;

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
	rfc->top[(opEpoch/2 + 1) % 2] = head;
	for (int i=0; i<NN; i++) { //maybe persist on line. check on optane
		char validOp = collectedValid[i];
		if (validOp != NONE) {
			pwbCounter5 += 2;
			// PWB(&(ANN(rfc, i, validOp)->val));
			// PWB(&(ANN(rfc, i, validOp)->epoch));
			PWB(&ANN(rfc, i, validOp));
			PWB(&rfc->announce_arr[i]->valid);
			// PWB(&rfc->announce_arr[i]);
		}
		
	}
	pwbCounter6 ++;
	PWB(&rfc->top[(opEpoch/2 + 1) % 2]);
	pfenceCounter3 ++;
	PFENCE();
	rfc->cEpoch = rfc->cEpoch + 1;
	pwbCounter7 ++;
	// this is important for the following case: the combiner updates the cEpoch, then several ops started to finish and return, 
	// BEFORE cEpoch is persisted. then, when the system recovers we can't distinguish between the following cases: 
	// 1. the combiner finished an operation and updated cEpoch (because it is not persisted), and several ops returned
	// 2. the combiner was in a middle of the combining session (for example).
	PWB(&rfc->cEpoch);
	pfenceCounter4 ++;
	PFENCE();
	rfc->cEpoch = rfc->cEpoch + 1;
	// pwbCounter8 ++;
	// PWB(&rfc->cEpoch); 
	// pfenceCounter5 ++;
	// PFENCE();
	// bool expected = true;
	cLock.store(false, std::memory_order_release);
	size_t value =  try_to_return(rfc, opEpoch, pid);
	return value;
}


size_t op(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, char opName, size_t param)
{
	size_t opEpoch = rfc->cEpoch;
	if (opEpoch % 2 == 1) {
		opEpoch ++;
	} 
	// announce
	char nextOp = toChar(1 - toDigit(rfc->announce_arr[pid]->valid));
	ANN(rfc, pid, nextOp)->val = NONE;
	ANN(rfc, pid, nextOp)->epoch = opEpoch; 
	ANN(rfc, pid, nextOp)->param = param;
    ANN(rfc, pid, nextOp)->name = opName;
	PWB(&ANN(rfc, pid, collectedValid[pid]));
	PFENCE();
	rfc->announce_arr[pid]->valid = nextOp;
	
	// valid = nextOp, 0 (dont collect nextOp)
	// pwb(valid)
	// pfence
	// valid = nextOp, 1 (collect nextOp)


	// rfc->announce_arr[pid]->param = param;
    // rfc->announce_arr[pid]->name = opName;
	// rfc->announce_arr[pid]->epoch = opEpoch;
	// rfc->announce_arr[pid]->val = NONE;
	
	// pwbCounter9[pid] ++;
	// std::atomic_fetch_add(&pwbCounter9, 1);
	// PWB(&rfc->announce_arr[pid]);
	
	// PWB(&rfc->announce_arr[pid]->valid);
	// PFENCE();

	// pfenceCounter6[pid] ++;
	// std::atomic_fetch_add(&pfenceCounter6, 1);
	
	// if after crash we see valid=true, we can be sure that all announcements were completed

	// rfc->announce_arr[pid]->valid = '1';

	size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	opEpoch = rfc->cEpoch;  // this is important for cases in which a late-arriving process eventually gets to be a combiner
	return combine(rfc, opEpoch, pop, pid);
}


// need to adapt recover
size_t recover(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, bool opName, size_t param)
{
	// valid = op, v
	// if v == 0 => change to 1
	// keep recovering regularly




	if (! rfc->announce_arr[pid]->valid) { 
		// did not announce properly
		return op(rfc, pop, pid, opName, param);
	}

    // size_t opEpoch = rfc->announce_arr[pid]->epoch;
    // size_t opVal = rfc->announce_arr[pid]->val;
	size_t opEpoch = VALID_ANN(rfc, pid)->epoch;
    size_t opVal = VALID_ANN(rfc, pid)->val;
	if (opVal != NONE and rfc->cEpoch >= opEpoch + 1) {
		return opVal;
	}
    size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	// garbage collect and update what nodes are free
    if (! garbage_collected) {
	    update_free_nodes(rfc, opEpoch);
        garbage_collected = true;
    }   
	return combine(rfc, opEpoch, pop, pid);
}


inline bool is_file_exists (const char* name) {
  struct stat buffer;   
  return (stat (name, &buffer) == 0); 
}

/**
 * enqueue-dequeue pairs: in each iteration a thread executes an enqueue followed by a dequeue;
 * the benchmark executes 10^8 pairs partitioned evenly among all threads;
 */
uint64_t pushPopTest(int numThreads, const long numPairs, const int numRuns) {
	const uint64_t kNumElements = 0; // Number of initial items in the stack
	static const long long NSEC_IN_SEC = 1000000000LL;
	
	pmem::obj::pool<root> pop;
	pmem::obj::persistent_ptr<root> proot;

	const char* pool_file_name = PM_FILE_NAME;

	pop = pool<root>::create(pool_file_name, "layout", PM_REGION_SIZE);
	proot = pop.root();
	transaction_allocations(proot, pop);
	std::cout << "Finished allocating!" << std::endl;

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
			op(proot->rfc, pop, tid, PUSH_OP, param);
			if (op(proot->rfc, pop, tid, POP_OP, NONE) == EMPTY) std::cout << "Error at measurement pop() iter=" << iter << "\n";
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
	};

	auto randop_lambda = [&numThreads, &startFlag,&numPairs, &proot, &pop](nanoseconds *delta, const int tid) {
		size_t param = tid;
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < 2 * numPairs/numThreads; iter++) {
			int randop = rand() % 2;         // randop in the range 0 to 1
			if (randop == 0) {
				op(proot->rfc, pop, tid, PUSH_OP, param);
			}
			else if (randop == 1) {
				op(proot->rfc, pop, tid, POP_OP, NONE);
			}
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
	};

	for (int irun = 0; irun < numRuns; irun++) {
		NN = numThreads;
		// currently, for each run there is one poolfile. therefore, only one run is supported

		// Fill the queue with an initial amount of nodes
		size_t param = size_t(41);
		for (uint64_t ielem = 0; ielem < kNumElements; ielem++) {
			op(proot->rfc, pop, 0, PUSH_OP, param);
		}
		std::thread enqdeqThreads[numThreads];
		// for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(randop_lambda, &deltas[tid][irun], tid);
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(pushpop_lambda, &deltas[tid][irun], tid);
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
	return (numPairs*2*NSEC_IN_SEC/median);
}


#define MILLION  1000000LL

int runSeveralTests() {
    const std::string dataFilename { DATA_FILE };
	// vector<int> threadList = { 1, 2, 4, 8, 10, 16, 24, 32, 40 };     // For Castor
    // std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40};     // For Castor
	std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60 };     // For Castor
    const int numRuns = 1;                                           // Number of runs
    const long numPairs = 1*MILLION;                                 // 1M is fast enough on the laptop

    uint64_t results[threadList.size()];
    std::string cName = "DFC";
    // Reset results
    std::memset(results, 0, sizeof(uint64_t)*threadList.size());

    // Enq-Deq Throughput benchmarks
    for (int it = 0; it < threadList.size(); it++) {
        int nThreads = threadList[it];
        std::cout << "\n----- pstack-ll (push-pop)   threads=" << nThreads << "   pairs=" << numPairs/MILLION << "M   runs=" << numRuns << " -----\n";
		results[it] = pushPopTest(nThreads, numPairs, numRuns);
		int sumPwbCounter9 = 0;
		// for (int i=0; i<NN; i++) {
		// 	sumPwbCounter9 += pwbCounter9[i];
		// 	pwbCounter9[i] = 0;
		// }
		pwbCounter = pwbCounter1 + pwbCounter2 + pwbCounter3 + pwbCounter4 + pwbCounter5+ pwbCounter6 + pwbCounter7 + pwbCounter8+ sumPwbCounter9 + pwbCounter10;
		std::cout << "#pwb/#op: " << pwbCounter / (numPairs*2) << std::endl;
		std::cout << "#pwb1/#op: " << pwbCounter1 / (numPairs*2) << std::endl;
		std::cout << "#pwb2/#op: " << pwbCounter2 / (numPairs*2) << std::endl;
		std::cout << "#pwb3/#op: " << pwbCounter3 / (numPairs*2) << std::endl;
		std::cout << "#pwb4/#op: " << pwbCounter4 / (numPairs*2) << std::endl;
		std::cout << "#pwb5/#op: " << pwbCounter5 / (numPairs*2) << std::endl;
		std::cout << "#pwb6/#op: " << pwbCounter6 / (numPairs*2) << std::endl;
		std::cout << "#pwb7/#op: " << pwbCounter7 / (numPairs*2) << std::endl;
		std::cout << "#pwb8/#op: " << pwbCounter8 / (numPairs*2) << std::endl;
		std::cout << "#pwb9/#op: " << sumPwbCounter9 / (numPairs*2) << std::endl;
		std::cout << "#pwb10/#op: " << pwbCounter10 / (numPairs*2) << std::endl;
		int sumPfenceCounter6 = 0;
		// for (int i=0; i<NN; i++) {
		// 	sumPfenceCounter6 += pfenceCounter6[i];
		// 	pfenceCounter6[i] = 0;
		// }
		pfenceCounter = pfenceCounter1 + pfenceCounter2 + pfenceCounter3 + pfenceCounter4 + pfenceCounter5+ sumPfenceCounter6 + pfenceCounter7;
		std::cout << "#pfence/#op: " << pfenceCounter / (numPairs*2) << std::endl;
		std::cout << "#pfence1/#op: " << pfenceCounter1 / (numPairs*2) << std::endl;
		std::cout << "#pfence2/#op: " << pfenceCounter2 / (numPairs*2) << std::endl;
		std::cout << "#pfence3/#op: " << pfenceCounter3 / (numPairs*2) << std::endl;
		std::cout << "#pfence4/#op: " << pfenceCounter4 / (numPairs*2) << std::endl;
		std::cout << "#pfence5/#op: " << pfenceCounter5 / (numPairs*2) << std::endl;
		std::cout << "#pfence6/#op: " << sumPfenceCounter6 / (numPairs*2) << std::endl;
		std::cout << "#pfence7/#op: " << pfenceCounter7 / (numPairs*2) << std::endl;
		pwbCounter = 0; pwbCounter1=0; pwbCounter2=0; pwbCounter3=0; pwbCounter4=0; pwbCounter5=0; pwbCounter6=0; pwbCounter7=0; pwbCounter8=0; pwbCounter10=0;
		pfenceCounter = 0; pfenceCounter1=0; pfenceCounter2=0; pfenceCounter3=0; pfenceCounter4=0; pfenceCounter5=0; pfenceCounter7=0;
		std::cout << "combiner total time: " << combineCounter.count() << std::endl;
		std::cout << "reduce total time: " << reduceCounter.count() << std::endl;
		std::cout << "find_free_node total time: " << findFreeCounter.count() << std::endl;
		combineCounter = 0ns; reduceCounter = 0ns; findFreeCounter = 0ns; 
    }

    // Export tab-separated values to a file to be imported in gnuplot or excel
    std::ofstream dataFile;
    dataFile.open(dataFilename);
    dataFile << "Threads\t";
    // Printf class names for each column
    dataFile << cName << "\t";
    dataFile << "\n";
    for (int it = 0; it < threadList.size(); it++) {
        dataFile << threadList[it] << "\t";
        dataFile << results[it] << "\t";
        dataFile << "\n";
    }
    dataFile.close();
    std::cout << "\nSuccessfuly saved results in " << dataFilename << "\n";

    return 0;
}



int main(int argc, char *argv[]) {
	// pmem::obj::pool<root> pop;
	// pmem::obj::persistent_ptr<root> proot;

	// const char* pool_file_name = PM_FILE_NAME;
	// if (is_file_exists(pool_file_name)) {
	// 		// open a pmemobj pool
	// 		pop = pool<root>::open(pool_file_name, "layout");
	// 		proot = pop.root();
	// 	}
	// 	else {
	// 	// create a pmemobj pool
	// 		// pop = pool<root>::create(pool_file_name, "layout", PMEMOBJ_MIN_POOL);
	// 		pop = pool<root>::create(pool_file_name, "layout", PM_REGION_SIZE);
	// 		proot = pop.root();
	// 		transaction_allocations(proot, pop);
	// 		std::cout << "Finished allocating!" << std::endl;
	// 	}
	runSeveralTests();

	// pmem::obj::pool<root> pop;
	// pmem::obj::persistent_ptr<root> proot;

	// const char* pool_file_name = "poolfile";
    // size_t params [N];
    // size_t ops [N];
    // std::thread threads_pool[N];

    // for (int pid=0; pid<N; pid++) {
    //         if (pid % 3 == 1) {
    //             params[pid] = NONE;
    //             ops[pid] = POP_OP;
    //         }
    //         else {
    //             params[pid] = pid;
    //             ops[pid] = PUSH_OP;
    //         }
    //     }

	// if (is_file_exists(pool_file_name)) {
	// 	// open a pmemobj pool
	// 	pop = pool<root>::open(pool_file_name, "layout");
	// 	proot = pop.root();

    //     for (int pid=0; pid<N; pid++) {
    //         threads_pool[pid] = std::thread (recover, proot->rfc, pop, pid, ops[pid], params[pid]);
    //     }							      
	// 	for (int pid=0; pid<N; pid++) {
	// 		threads_pool[pid].join();
	// 	}
	// 	print_state(proot->rfc);
		
		
	// 	for (int pid=0; pid<N; pid++) {
    //         threads_pool[pid] = std::thread (op, proot->rfc, pop, pid, ops[pid], params[pid]);
    //     }							      
	// 	for (int pid=0; pid<N; pid++) {
	// 		threads_pool[pid].join();
	// 	}
	// 	print_state(proot->rfc);
	// }
	// else {
	// 	// create a pmemobj pool
	// 	pop = pool<root>::create(pool_file_name, "layout", PMEMOBJ_MIN_POOL);
	// 	proot = pop.root();
	// 	transaction_allocations(proot, pop);
	// 	std::cout << "Finished allocating!" << std::endl;
		
	// 	for (int pid=0; pid<N; pid++) {
    //         threads_pool[pid] = std::thread (op, proot->rfc, pop, pid, ops[pid], params[pid]);
    //     }
	// 	// usleep(1);
	// 	kill(getpid(), SIGKILL);
	// 	for (int pid=0; pid<N; pid++) {
	// 		threads_pool[pid].join();
	// 	}
	// 	print_state(proot->rfc);
	// }
}