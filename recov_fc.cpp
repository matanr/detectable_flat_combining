/*
 * recov_fc.cpp -- recoverable flat combining implemented using libpmemobj C++ bindings
 */
#include <sys/stat.h>
#include <bits/stdc++.h>
#include <atomic>
#include <cstring>
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

using namespace pmem;
using namespace pmem::obj;
using namespace std::chrono;
using namespace std::literals::chrono_literals;

#ifndef DATA_FILE
#define DATA_FILE "other_platforms/data/pstack-ll-rfc.txt"
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

#define N 40  // number of processes
int NN = 40;  // number of processes running now
#define MAX_POOL_SIZE 40  // number of nodes in the pool
#define ACK -1
#define EMPTY -2
#define NONE -3
#define PUSH_OP true
#define POP_OP false


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

int pwbCounter = 0; int pwbCounter1=0; int pwbCounter2=0; int pwbCounter3=0; int pwbCounter4=0; int pwbCounter5=0; int pwbCounter6=0; int pwbCounter7=0; int pwbCounter8=0; int pwbCounter9=0; int pwbCounter10=0;
int pfenceCounter = 0; int pfenceCounter1=0; int pfenceCounter2=0; int pfenceCounter3=0; int pfenceCounter4=0; int pfenceCounter5=0; int pfenceCounter6=0; int pfenceCounter7=0;
nanoseconds combineCounter = 0ns; nanoseconds reduceCounter = 0ns; nanoseconds findFreeCounter = 0ns; 

struct alignas(64) announce {
    p<size_t> val;
    p<size_t> epoch;
	p<bool> name;
    p<size_t> param; 
	p<bool> valid;
} ;

struct node {
    p<size_t> param;
    persistent_ptr<node> next;
    p<bool> is_free;
} ;

struct recoverable_fc {
	p<size_t> cEpoch = 0;
	persistent_ptr<announce> announce_arr [N];
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
			proot->rfc->announce_arr[pid] = make_persistent<announce>();
			proot->rfc->announce_arr[pid]->val = NONE;
			proot->rfc->announce_arr[pid]->epoch = 0;
			proot->rfc->announce_arr[pid]->name = NONE;
			proot->rfc->announce_arr[pid]->param = NONE;
			proot->rfc->announce_arr[pid]->valid = false;
		}
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			proot->rfc->nodes_pool[i] = make_persistent<node>();
			proot->rfc->nodes_pool[i]->param = NONE;
			proot->rfc->nodes_pool[i]->next = NULL;
			proot->rfc->nodes_pool[i]->is_free = true;
		} 
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
    size_t val = rfc->announce_arr[pid]->val;
    if (val == NONE) { 
		opEpoch += 2;
		return try_to_take_lock(rfc, opEpoch, pid);
	}
	else {
		return val;
	}
}


std::list<size_t> reduce(persistent_ptr<recoverable_fc> rfc) {
	auto startBeats = steady_clock::now();
	std::list<size_t> opsList;
	if (rfc->cEpoch%2 == 1) {
		rfc->cEpoch = rfc->cEpoch + 1;
	}
	pwbCounter1 ++;
	PWB(&rfc->cEpoch);
	pfenceCounter1 ++;
	PFENCE(); 
	for (size_t i = 0; i < NN; i++) {
		if (rfc->announce_arr[i] == NULL) {
			continue;
		}
		size_t opEpoch = rfc->announce_arr[i]->epoch;
		size_t opVal = rfc->announce_arr[i]->val;
		bool isOpValid = rfc->announce_arr[i]->valid;
		if (isOpValid && (opEpoch == rfc->cEpoch || opVal == NONE)) {
			rfc->announce_arr[i]->epoch = rfc->cEpoch;
			// pwbCounter2 ++;
			// PWB(&rfc->announce_arr[i]);
			// PWB(&rfc->announce_arr[i]->epoch);

            bool opName = rfc->announce_arr[i]->name;
            size_t opParam = rfc->announce_arr[i]->param;
			if (opName == PUSH_OP) {
				if (opsList.empty()) {
					opsList.push_front(i);
				}
				else {
					size_t cId = opsList.front();
					bool cOp = rfc->announce_arr[cId]->name;
					if (cOp == PUSH_OP) {
						opsList.push_front(i);
					}
					else {
						// std::cout << "merging push with prev pop. opsList size: " << opsList.size() << std::endl;
						rfc->announce_arr[i]->val = ACK;
						rfc->announce_arr[cId]->val = opParam;
						opsList.pop_front();
						// std::cout << "opsList size: " << opsList.size() << std::endl;
					}
				}
			}
			else if (opName == POP_OP) {
				if (opsList.empty()) {
					opsList.push_front(i);
				}
				else {
					size_t cId = opsList.front();
					bool cOp = rfc->announce_arr[cId]->name;
					if (cOp == POP_OP) {
						opsList.push_front(i);
					}
					else {
						rfc->announce_arr[cId]->val = ACK;
						size_t pushParam = rfc->announce_arr[cId]->param;
						rfc->announce_arr[i]->val = pushParam;
						opsList.pop_front();
					}
				}
			}
		}
	}
	auto stopBeats = steady_clock::now();
	reduceCounter += stopBeats - startBeats;
	return opsList; // empty list
}

std::list<size_t> reduce2lists(persistent_ptr<recoverable_fc> rfc) {
	auto startBeats = steady_clock::now();
	std::list<size_t> pushList, popList;
	if (rfc->cEpoch%2 == 1) {
		rfc->cEpoch = rfc->cEpoch + 1;
	}
	pwbCounter1 ++;
	PWB(&rfc->cEpoch);
	pfenceCounter1 ++;
	PFENCE(); 
	for (size_t i = 0; i < NN; i++) {
		if (rfc->announce_arr[i] == NULL) {
			continue;
		}
		size_t opEpoch = rfc->announce_arr[i]->epoch;
		size_t opVal = rfc->announce_arr[i]->val;
		bool isOpValid = rfc->announce_arr[i]->valid;
		if (isOpValid && (opEpoch == rfc->cEpoch || opVal == NONE)) {
			rfc->announce_arr[i]->epoch = rfc->cEpoch;
            bool opName = rfc->announce_arr[i]->name;
            size_t opParam = rfc->announce_arr[i]->param;
			if (opName == PUSH_OP) {
				pushList.push_back(i);
			}
			else if (opName == POP_OP) {
				popList.push_back(i);
			}
			pwbCounter2 ++;
			// PWB(&rfc->announce_arr[i]);
			PWB(&rfc->announce_arr[i]->epoch);
		}
	}
	// pfenceCounter2 ++;
	// PFENCE();
	while((!pushList.empty()) || (!popList.empty())) {
		auto cPush = pushList.front();
		auto cPop = popList.front();
		if ((!pushList.empty()) && (!popList.empty())) {
            rfc->announce_arr[cPush]->val = ACK;
			size_t pushParam = rfc->announce_arr[cPush]->param;
            rfc->announce_arr[cPop]->val = pushParam;

			pushList.pop_front();
			popList.pop_front();
		}
		else if (!pushList.empty()) {
			auto stopBeats = steady_clock::now();
			reduceCounter += stopBeats - startBeats;
			return pushList;
		}
		else {
			auto stopBeats = steady_clock::now();
			reduceCounter += stopBeats - startBeats;
			return popList;
		}
	}
	auto stopBeats = steady_clock::now();
	reduceCounter += stopBeats - startBeats;
	return pushList; // empty list
}

// garbage collection, updates is_free for all nodes in the pool
void update_free_nodes(persistent_ptr<recoverable_fc> rfc, size_t opEpoch) {
	// each node is free, unless current top is somehow points to it
	for (int i=0; i<MAX_POOL_SIZE; i++) {
		rfc->nodes_pool[i]->is_free = true;
	}
	int notfree_count = 0;
	auto current = rfc->top[(opEpoch/2)%2];
	while (current != NULL) {
		current->is_free = false;
		current = current->next;
		notfree_count ++;
	}
}


// after crash, combiner must run garbage collection, and update the is_free field of each node in the pool
int find_free_node(persistent_ptr<recoverable_fc> rfc, int current_index=0) {
	auto startBeats = steady_clock::now();
	for (int i=current_index; i<MAX_POOL_SIZE; i++) {
		if (rfc->nodes_pool[i]->is_free)  {
			auto stopBeats = steady_clock::now();
			findFreeCounter += stopBeats - startBeats;
			return i;
		}
	}
	auto stopBeats = steady_clock::now();
	findFreeCounter += stopBeats - startBeats;
	return -1;
}

int count_free_nodes(persistent_ptr<recoverable_fc> rfc) {
	int counter = 0;
	for (int i=0; i<MAX_POOL_SIZE; i++) {
		if (rfc->nodes_pool[i]->is_free)  {
			counter ++;
		}
	}
	return counter;
}


size_t combine(persistent_ptr<recoverable_fc> rfc, size_t opEpoch, pmem::obj::pool<root> pop, size_t pid) {
	auto startBeats = steady_clock::now();
	std::list<size_t> l = reduce(rfc);
	// std::cout << "Combiner reduced:";
	// for (auto v : l) {
	// 	size_t cOp = rfc->announce_arr[v]->name;
	// 	if (cOp == PUSH_OP) {
	// 		std::cout << " " << v << "(push)";
	// 	} else{
	// 		std::cout << " " << v << "(pop)";
	// 	}
	// }
	// std::cout << std::endl;
	// std::cout << ", opEpoch is: " << opEpoch << std::endl;
	persistent_ptr<node> head = rfc->top[(opEpoch/2)%2];
	if (!l.empty()) {
		// std::cout << "l not empty" << std::endl;
		size_t cId = l.front();
		bool cOp = rfc->announce_arr[cId]->name;
		if (cOp == PUSH_OP) {
			int freeIndexLowerLim = 0;
			do {
				int freeIndex = find_free_node(rfc, freeIndexLowerLim);
				if (freeIndex == -1) {
					std::cerr << "Nodes pool is too small" << std::endl;
					exit(-1);
				}
				freeIndexLowerLim ++;
				auto newNode = rfc->nodes_pool[freeIndex];
				size_t newParam = rfc->announce_arr[cId]->param;
				newNode->param = newParam;
				newNode->next = head;
				newNode->is_free = false;
                rfc->announce_arr[cId]->val = ACK;
				pwbCounter3 ++;
				PWB(&newNode);
				head = newNode;
				l.pop_front();
				cId = l.front();
			} while (!l.empty());
			rfc->top[(opEpoch/2 + 1)%2] = head;
		}
		else {
			do {
				if (head == NULL) {
                    rfc->announce_arr[cId]->val = EMPTY;
				}
				else {
                    size_t headParam = head->param;
					rfc->announce_arr[cId]->val = headParam;
					head->is_free = true;
					pwbCounter4 ++;
					PWB(&head);
					head = head->next;
				}
				l.pop_front();
				cId = l.front();
			} while (!l.empty());
			rfc->top[(opEpoch/2 + 1) % 2] = head;
		}		
	}
    else { // important !
        rfc->top[(opEpoch/2 + 1) % 2] = rfc->top[(opEpoch/2) % 2];
    }
	for (int i=0;i<NN;i++) {
		size_t currentEpoch = rfc->cEpoch;
		if (rfc->announce_arr[i]->val != NONE && rfc->announce_arr[i]->epoch == currentEpoch) {
			pwbCounter5 ++;
			PWB(&rfc->announce_arr[pid]);
		}
	}
	pwbCounter6 ++;
	PWB(&rfc->top[(opEpoch/2 + 1) % 2]);
	pfenceCounter3 ++;
	PFENCE();
	rfc->cEpoch = rfc->cEpoch + 1;
	pwbCounter7 ++;
	PWB(&rfc->cEpoch);
	pfenceCounter4 ++;
	PFENCE();
	rfc->cEpoch = rfc->cEpoch + 1;
	pwbCounter8 ++;
	PWB(&rfc->cEpoch); 
	pfenceCounter5 ++;
	PFENCE();
	bool expected = true;
	// bool combiner = cLock.compare_exchange_strong(expected, false, std::memory_order_release, std::memory_order_relaxed);
	auto stopBeats = steady_clock::now();
	combineCounter += stopBeats - startBeats;
	cLock.store(false, std::memory_order_release);
	size_t value =  try_to_return(rfc, opEpoch, pid);
	// std::cout << "after try_to_return" << std::endl;
	return value;
}


size_t op(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, bool opName, size_t param)
{
	size_t opEpoch = rfc->cEpoch;
	if (opEpoch % 2 == 1) {
		opEpoch ++;
	} 
	// announce
	rfc->announce_arr[pid]->valid = false;
	// asm volatile("mfence" ::: "memory");
	pwbCounter9 ++;
	PWB(&rfc->announce_arr[pid]->valid);
	pfenceCounter6 ++;
	PFENCE(); 
    rfc->announce_arr[pid]->val = NONE;
	rfc->announce_arr[pid]->epoch = opEpoch;
	rfc->announce_arr[pid]->param = param;
    rfc->announce_arr[pid]->name = opName;
	pwbCounter10 ++;
	PWB(&rfc->announce_arr[pid]);
	pfenceCounter7 ++;
	PFENCE();
	rfc->announce_arr[pid]->valid = true;
	// asm volatile("mfence" ::: "memory");

	size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	opEpoch = rfc->cEpoch;
	// std::cout << "~~~ Combiner is: " << pid << " ~~~" << std::endl;
	return combine(rfc, opEpoch, pop, pid);
}

size_t recover(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, bool opName, size_t param)
{
	if (! rfc->announce_arr[pid]->valid) {
		// did not announce properly
		return op(rfc, pop, pid, opName, param);
	}

    size_t opEpoch = rfc->announce_arr[pid]->epoch;
    size_t opVal = rfc->announce_arr[pid]->val;
	if (opVal != NONE and rfc->cEpoch >= opEpoch + 1) {
		return opVal;
	}
    size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	// garbage collect and update the is_free of nodes
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
uint64_t pushPopTest(pmem::obj::pool<root> pop, pmem::obj::persistent_ptr<root> proot, int numThreads, const long numPairs, const int numRuns) {
	const uint64_t kNumElements = 0; // Number of initial items in the stack
	static const long long NSEC_IN_SEC = 1000000000LL;
	
	// pmem::obj::pool<root> pop;
	// pmem::obj::persistent_ptr<root> proot;

	// const char* pool_file_name = "poolfile";
	const char* pool_file_name = PM_FILE_NAME;
    size_t params [N];
    size_t ops [N];
    std::thread threads_pool[N];

	std::cout << "in push pop" << std::endl;
	nanoseconds deltas[numThreads][numRuns];
	std::atomic<bool> startFlag = { false };

	std::cout << "##### " << "Recoverable FC" << " #####  \n";

	auto pushpop_lambda = [&numThreads, &startFlag,&numPairs, &proot, &pop](nanoseconds *delta, const int tid) {
		//UserData* ud = new UserData{0,0};
		size_t param = tid;
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < numPairs/numThreads; iter++) {
			op(proot->rfc, pop, tid, PUSH_OP, param);
			// print_state(proot->rfc);
			if (op(proot->rfc, pop, tid, POP_OP, NONE) == EMPTY) std::cout << "Error at measurement pop() iter=" << iter << "\n";
			// print_state(proot->rfc);
			// int counter = count_free_nodes(proot->rfc);
			// if (counter < 40) {
			// 	std::cout << "free nodes: " << counter << std::endl;
			// 	std::cout << "# free nodes lower than 40. My tid is: " << tid << std::endl;
			// 	if (counter < 39) exit(-1);
				

			// }
			
			
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
		// threads_pool[tid] = std::thread (op, proot->rfc, pop, tid, PUSH_OP, params[tid]);
		// threads_pool[tid] = std::thread (op, proot->rfc, pop, tid, POP_OP, params[tid]);
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = std::thread(pushpop_lambda, &deltas[tid][irun], tid);
		startFlag.store(true);
		// Sleep for 2 seconds just to let the threads see the startFlag
		std::this_thread::sleep_for(2s);
		for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid].join();
		startFlag.store(false);
		// should delete poolfile afterwards here
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

int runSeveralTests(pmem::obj::pool<root> pop, pmem::obj::persistent_ptr<root> proot) {
    const std::string dataFilename { DATA_FILE };
    std::vector<int> threadList = { 1, 2, 4, 8, 10, 16, 24, 32, 40 };     // For Castor
	// std::vector<int> threadList = {2, 4, 8, 10, 16, 24, 32, 40 };     // For Castor
    const int numRuns = 1;                                           // Number of runs
    const long numPairs = 1*MILLION;                                 // 1M is fast enough on the laptop

    uint64_t results[threadList.size()];
    std::string cName;
    // Reset results
    std::memset(results, 0, sizeof(uint64_t)*threadList.size());

    // Enq-Deq Throughput benchmarks
    for (int it = 0; it < threadList.size(); it++) {
        int nThreads = threadList[it];
        std::cout << "\n----- pstack-ll (push-pop)   threads=" << nThreads << "   pairs=" << numPairs/MILLION << "M   runs=" << numRuns << " -----\n";
		results[it] = pushPopTest(pop, proot, nThreads, numPairs, numRuns);
		pwbCounter = pwbCounter1 + pwbCounter2 + pwbCounter3 + pwbCounter4 + pwbCounter5+ pwbCounter6 + pwbCounter7 + pwbCounter8+ pwbCounter9 + pwbCounter10;
		std::cout << "#pwb/#op: " << pwbCounter / (numPairs*2) << std::endl;
		std::cout << "#pwb1/#op: " << pwbCounter1 / (numPairs*2) << std::endl;
		std::cout << "#pwb2/#op: " << pwbCounter2 / (numPairs*2) << std::endl;
		std::cout << "#pwb3/#op: " << pwbCounter3 / (numPairs*2) << std::endl;
		std::cout << "#pwb4/#op: " << pwbCounter4 / (numPairs*2) << std::endl;
		std::cout << "#pwb5/#op: " << pwbCounter5 / (numPairs*2) << std::endl;
		std::cout << "#pwb6/#op: " << pwbCounter6 / (numPairs*2) << std::endl;
		std::cout << "#pwb7/#op: " << pwbCounter7 / (numPairs*2) << std::endl;
		std::cout << "#pwb8/#op: " << pwbCounter8 / (numPairs*2) << std::endl;
		std::cout << "#pwb9/#op: " << pwbCounter9 / (numPairs*2) << std::endl;
		std::cout << "#pwb10/#op: " << pwbCounter10 / (numPairs*2) << std::endl;
		pfenceCounter = pfenceCounter1 + pfenceCounter2 + pfenceCounter3 + pfenceCounter4 + pfenceCounter5+ pfenceCounter6 + pfenceCounter7;
		std::cout << "#pfence/#op: " << pfenceCounter / (numPairs*2) << std::endl;
		std::cout << "#pfence1/#op: " << pfenceCounter1 / (numPairs*2) << std::endl;
		std::cout << "#pfence2/#op: " << pfenceCounter2 / (numPairs*2) << std::endl;
		std::cout << "#pfence3/#op: " << pfenceCounter3 / (numPairs*2) << std::endl;
		std::cout << "#pfence4/#op: " << pfenceCounter4 / (numPairs*2) << std::endl;
		std::cout << "#pfence5/#op: " << pfenceCounter5 / (numPairs*2) << std::endl;
		std::cout << "#pfence6/#op: " << pfenceCounter6 / (numPairs*2) << std::endl;
		std::cout << "#pfence7/#op: " << pfenceCounter7 / (numPairs*2) << std::endl;
		pwbCounter = 0; pwbCounter1=0; pwbCounter2=0; pwbCounter3=0; pwbCounter4=0; pwbCounter5=0; pwbCounter6=0; pwbCounter7=0; pwbCounter8=0; pwbCounter9=0; pwbCounter10=0;
		pfenceCounter = 0; pfenceCounter1=0; pfenceCounter2=0; pfenceCounter3=0; pfenceCounter4=0; pfenceCounter5=0; pfenceCounter6=0; pfenceCounter7=0;
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
	pmem::obj::pool<root> pop;
	pmem::obj::persistent_ptr<root> proot;

	const char* pool_file_name = PM_FILE_NAME;
	if (is_file_exists(pool_file_name)) {
			// open a pmemobj pool
			pop = pool<root>::open(pool_file_name, "layout");
			proot = pop.root();
		}
		else {
		// create a pmemobj pool
			// pop = pool<root>::create(pool_file_name, "layout", PMEMOBJ_MIN_POOL);
			pop = pool<root>::create(pool_file_name, "layout", PM_REGION_SIZE);
			proot = pop.root();
			transaction_allocations(proot, pop);
			std::cout << "Finished allocating!" << std::endl;
		}
	runSeveralTests(pop, proot);
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