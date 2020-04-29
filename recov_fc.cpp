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
#define DATA_FILE "data/pstack-ll-rfc.txt"
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
#define MAX_POOL_SIZE 700  // number of nodes in the pool
#define ACK -1
#define EMPTY -2
#define NONE -3
#define PUSH_OP -4
#define POP_OP -5

std::atomic<bool> cLock {false};    // holds true when locked, holds false when unlocked
bool garbage_collected = false;

struct alignas(64) announce {
    p<size_t> val;
    p<size_t> epoch;
	p<size_t> name;
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
	persistent_ptr<node> top [2] = {NULL, NULL};
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
    while (current != NULL) {
        std::cout << "Param: " << current->param << std::endl;
        current = current->next;
    }
}


void transaction_allocations(persistent_ptr<root> proot, pmem::obj::pool<root> pop) {
	transaction::run(pop, [&] {
		// allocation
		proot->rfc = make_persistent<recoverable_fc>();
	
		for (int pid=0; pid<N; pid++) {
			if (proot->rfc->announce_arr[pid] == NULL) {
				proot->rfc->announce_arr[pid] = make_persistent<announce>();
				// proot->rfc->announce_arr[pid]->res = make_persistent<result>();
				// proot->rfc->announce_arr[pid]->op = make_persistent<operation>();
			}
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
			sleep(0);
			if (cLock == false && rfc->cEpoch <= opEpoch + 1){
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
	std::list<size_t> pushList, popList;
	if (rfc->cEpoch%2 == 1) {
		rfc->cEpoch = rfc->cEpoch + 1;
	}
	pmem_flush(&rfc->cEpoch, sizeof(&rfc->cEpoch)); 
	pmem_drain();
	for (size_t i = 0; i < N; i++) {
		if (rfc->announce_arr[i] == NULL) {
			continue;
		}
		size_t opEpoch = rfc->announce_arr[i]->epoch;
		size_t opVal = rfc->announce_arr[i]->val;
		bool isOpValid = rfc->announce_arr[i]->valid;
		if (isOpValid && (opEpoch == rfc->cEpoch || opVal == NONE)) {
			rfc->announce_arr[i]->epoch = rfc->cEpoch;
            size_t opName = rfc->announce_arr[i]->name;
            size_t opParam = rfc->announce_arr[i]->param;
			if (opName == PUSH_OP) {
				pushList.push_back(i);
			}
			else if (opName == POP_OP) {
				popList.push_back(i);
			}
			pmem_flush(&rfc->announce_arr[i], sizeof(&rfc->announce_arr[i])); 
		}
	}
	pmem_drain();
	while((!pushList.empty()) || (!popList.empty())) {
		auto cPush = pushList.front();
		auto cPop = popList.front();
		if ((!pushList.empty()) && (!popList.empty())) {
            rfc->announce_arr[cPush]->val = ACK;
            rfc->announce_arr[cPop]->val = rfc->announce_arr[cPush]->param;

			pushList.pop_front();
			popList.pop_front();
		}
		else if (!pushList.empty()) {
			return pushList;
		}
		else {
			return popList;
		}
	}
	return pushList; // empty list
}

// garbage collection, updates is_free for all nodes in the pool
void update_free_nodes(persistent_ptr<recoverable_fc> rfc, size_t opEpoch) {
	// each node is free, unless current top is somehow points to it
	for (int i=0; i<MAX_POOL_SIZE; i++) {
		rfc->nodes_pool[i]->is_free = true;
	}
	auto current = rfc->top[(opEpoch/2)%2];
	while (current != NULL) {
		current->is_free = false;
		current = current->next;
	}
}


// after crash, combiner must run garbage collection, and update the is_free field of each node in the pool
int find_free_node(persistent_ptr<recoverable_fc> rfc, int current_index=0) {
	for (int i=current_index; i<MAX_POOL_SIZE; i++) {
		if (rfc->nodes_pool[i]->is_free)  {
			return i;
		}
	}
	return -1;
}


size_t combine(persistent_ptr<recoverable_fc> rfc, size_t opEpoch, pmem::obj::pool<root> pop, size_t pid)
{
	std::list<size_t> l = reduce(rfc);
	// std::cout << "Combiner reduced:";
	// for (auto v : l) {
	// 	std::cout << " " << v;
	// }
	// std::cout << ", opEpoch is: " << opEpoch << std::endl;
	if (!l.empty()) {
		size_t cId = l.front();
		size_t cOp = rfc->announce_arr[cId]->name;
		persistent_ptr<node> head = rfc->top[(opEpoch/2)%2];
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
				newNode->param = rfc->announce_arr[cId]->param;
				newNode->next = head;
				newNode->is_free = false;
                rfc->announce_arr[cId]->val = ACK;
				head = newNode;
				pmem_flush(&newNode, sizeof(&newNode));  // CLWB
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
                    rfc->announce_arr[cId]->val = head->param;
				}
				head->is_free = true;
                head = head->next;
				l.pop_front();
				cId = l.front();
			} while (!l.empty());
			rfc->top[(opEpoch/2 + 1) % 2] = head;
		}		
	}
    else { // important !
        rfc->top[(opEpoch/2 + 1) % 2] = rfc->top[(opEpoch/2) % 2];
    }
	for (int i=0;i<N;i++) {
		pmem_flush(&rfc->announce_arr[pid], sizeof(&rfc->announce_arr[pid]));  // CLWB	
	}
	pmem_flush(&rfc->top[(opEpoch/2 + 1) % 2], sizeof(&rfc->top[(opEpoch/2 + 1) % 2]));  // CLWB
	pmem_drain(); // SFENCE
	rfc->cEpoch = rfc->cEpoch + 1;
	pmem_flush(&rfc->cEpoch, sizeof(&rfc->cEpoch)); // CLWB
	pmem_drain(); // SFENCE
	rfc->cEpoch = rfc->cEpoch + 1;
	pmem_flush(&rfc->cEpoch, sizeof(&rfc->cEpoch)); 
	pmem_drain();
	// std::cout << "Combiner updated cEpoch to " << rfc->cEpoch << std::endl;
	bool expected = true;
	bool combiner = cLock.compare_exchange_strong(expected, false, std::memory_order_release, std::memory_order_relaxed);
	size_t value =  try_to_return(rfc, opEpoch, pid);
	return value;
}


size_t op(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, size_t opName, size_t param)
{
	size_t opEpoch = rfc->cEpoch;
	if (opEpoch % 2 == 1) {
		opEpoch ++;
	} 

	// announce
	rfc->announce_arr[pid]->valid = false;
	pmem_flush(&rfc->announce_arr[pid]->valid, sizeof(&rfc->announce_arr[pid]->valid)); // CLWB
	pmem_drain(); // SFENCE
    rfc->announce_arr[pid]->val = NONE;
	rfc->announce_arr[pid]->epoch = opEpoch;
	rfc->announce_arr[pid]->param = param;
    rfc->announce_arr[pid]->name = opName;
	pmem_flush(&rfc->announce_arr[pid], sizeof(&rfc->announce_arr[pid])); // CLWB
	pmem_drain(); // SFENCE
	rfc->announce_arr[pid]->valid = true;

	size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	// std::cout << "~~~ Combiner is: " << pid << " ~~~" << std::endl;
	return combine(rfc, opEpoch, pop, pid);
}

size_t recover(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, size_t opName, size_t param)
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
		size_t param = 42;
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

	for (int irun = 0; irun < numRuns; irun++) {
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