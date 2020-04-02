/*
 * array.cpp -- recoverable flat combining implemented using libpmemobj C++ bindings
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
// #include <libpmemobj++/container/string.hpp>
#include <libpmem.h>
// #include <sstream>
// #include <string> 

using namespace pmem;
using namespace pmem::obj;

#define N 32  // number of processes
#define MAX_POOL_SIZE 100  // number of nodes in the pool
#define ACK -1
#define EMPTY -2
#define NONE -3

std::atomic<bool> cLock {false};    // holds true when locked, holds false when unlocked
bool garbage_collected = false;

struct result {
    p<size_t> val;
    p<size_t> epoch;
} ;

struct operation {
    p<const char *> name;
    p<size_t> param; 
} ;

struct announce {
    persistent_ptr<result> res;
    persistent_ptr<operation> op;
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

void allocate_announce(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid) {
	if (rfc->announce_arr[pid] == NULL) {
		transaction::run(pop, [&] {
			rfc->announce_arr[pid] = make_persistent<announce>();

			rfc->announce_arr[pid]->res = make_persistent<result>();
			rfc->announce_arr[pid]->op = make_persistent<operation>();
		});
	}
    rfc->announce_arr[pid]->res->val = NONE;
	rfc->announce_arr[pid]->res->epoch = 0;
	rfc->announce_arr[pid]->op->name = NULL;
	rfc->announce_arr[pid]->op->param = NONE;
}

void allocate_nodes_pool(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop) {
	transaction::run(pop, [&] {
		for (int i=0; i < MAX_POOL_SIZE; i++) {
			rfc->nodes_pool[i] = make_persistent<node>();
			rfc->nodes_pool[i]->param = NONE;
			rfc->nodes_pool[i]->next = NULL;
			rfc->nodes_pool[i]->is_free = true;
		} 
	});
}

size_t lock_taken(persistent_ptr<recoverable_fc> rfc, size_t & opEpoch, bool combiner, size_t pid)
{
	if (combiner == false) {
		while (rfc->cEpoch <= opEpoch + 1) {
			sleep(0);
			if (cLock == false && rfc->cEpoch <= opEpoch + 1){
				// std::cout << "lock_taken, in if, pid: " << pid << ", opEpoch: " << opEpoch << std::endl;
                return try_to_take_lock(rfc, opEpoch, pid);
			}
		}
		// std::cout << "lock_taken, after while, pid: " << pid << ", opEpoch: " << opEpoch << std::endl;
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
    size_t val = rfc->announce_arr[pid]->res->val;
    // std::string val = rfc->announce_arr[pid]->res->val.load(std::memory_order_relaxed);
    if (val == NONE) { 
		opEpoch += 2; 
		// std::cout << "in try_to_return, opEpoch +=2, pid: " << pid <<std::endl;
		return try_to_take_lock(rfc, opEpoch, pid);
	}
	else {
		return val;
	}
}


std::list<size_t> reduce(persistent_ptr<recoverable_fc> rfc) 
{
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
		size_t opEpoch = rfc->announce_arr[i]->res->epoch;
		size_t opVal = rfc->announce_arr[i]->res->val;
		// std::cout << "op " << i << "opEpoch is: " << opEpoch << "opVal is: " << opVal << std::endl;
		if (opEpoch == rfc->cEpoch || opVal == NONE) {
			rfc->announce_arr[i]->res->epoch = rfc->cEpoch;
            const char * opName = rfc->announce_arr[i]->op->name;
			// std::cout << "op " << i << "opName: " << opName << std::endl;
            size_t opParam = rfc->announce_arr[i]->op->param;
            // std::cout << "param: " << opParam  << std::endl;
			if (opName == "Push") {
				// std::cout << "pushlist " << i << std::endl;
				pushList.push_back(i);
			}
			else if (opName == "Pop") {
				popList.push_back(i);
				// std::cout << "poplist " << i << std::endl;
			}
		}
		pmem_flush(&rfc->announce_arr[i], sizeof(&rfc->announce_arr[i])); 
	}
	pmem_drain();
	while((!pushList.empty()) || (!popList.empty())) {
		auto cPush = pushList.front();
		auto cPop = popList.front();
		if ((!pushList.empty()) && (!popList.empty())) {
            rfc->announce_arr[cPush]->res->val = ACK;
            rfc->announce_arr[cPop]->res->val = rfc->announce_arr[cPush]->op->param;

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
	std::cout << "Combiner reduced:";
	for (auto v : l) {
		std::cout << " " << v;
	}
	std::cout << ", opEpoch is: " << opEpoch << std::endl;
	if (!l.empty()) {
		size_t cId = l.front();
		const char * cOp = rfc->announce_arr[cId]->op->name;
		persistent_ptr<node> head = rfc->top[(opEpoch/2)%2];
		if (cOp == "Push") {
			int freeIndexLowerLim = 0;
			do {
				int freeIndex = find_free_node(rfc, freeIndexLowerLim);
				if (freeIndex == -1) {
					std::cerr << "Nodes pool is too small" << std::endl;
					exit(-1);
				}
				freeIndexLowerLim ++;
				auto newNode = rfc->nodes_pool[freeIndex];
				newNode->param = rfc->announce_arr[cId]->op->param;
				newNode->next = head;
				newNode->is_free = false;
				// rfc->announce_arr[cId]->res->val.store("ack", std::memory_order_relaxed); //'ack'
                rfc->announce_arr[cId]->res->val = ACK;
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
					// rfc->announce_arr[cId]->res->val.store("empty", std::memory_order_relaxed);
                    rfc->announce_arr[cId]->res->val = EMPTY;
				}
				else {
					// rfc->announce_arr[cId]->res->val.store(head->param, std::memory_order_relaxed);
                    rfc->announce_arr[cId]->res->val = head->param;
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
		pmem_flush(&rfc->announce_arr[pid]->res, sizeof(&rfc->announce_arr[pid]->res));  // CLWB	
	}
	pmem_flush(&rfc->top[(opEpoch/2 + 1) % 2], sizeof(&rfc->top[(opEpoch/2 + 1) % 2]));  // CLWB
	pmem_drain(); // SFENCE
	rfc->cEpoch = rfc->cEpoch + 1;
	pmem_flush(&rfc->cEpoch, sizeof(&rfc->cEpoch)); // CLWB
	pmem_drain(); // SFENCE
	rfc->cEpoch = rfc->cEpoch + 1;
	pmem_flush(&rfc->cEpoch, sizeof(&rfc->cEpoch)); 
	pmem_drain();
	std::cout << "Combiner updated cEpoch to " << rfc->cEpoch << std::endl;
    // print_state(rfc);
	bool expected = true;
	bool combiner = cLock.compare_exchange_strong(expected, false, std::memory_order_release, std::memory_order_relaxed);
	// std::cout << "combiner released cas= " << cLock << std::endl;
	size_t value =  try_to_return(rfc, opEpoch, pid);
    // std::cout << "after try_to_return " << std::endl;
	return value;
}


size_t op(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, const char * opName, size_t param)
{
	size_t opEpoch = rfc->cEpoch;
	if (opEpoch % 2 == 1) {
		opEpoch ++;
	} 
    rfc->announce_arr[pid]->res->val = NONE;
	rfc->announce_arr[pid]->res->epoch = opEpoch;
    rfc->announce_arr[pid]->op->param = param;
    rfc->announce_arr[pid]->op->name = opName;

	size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	std::cout << "~~~ Combiner is: " << pid << " ~~~" << std::endl;
	return combine(rfc, opEpoch, pop, pid);
}

size_t recover(persistent_ptr<recoverable_fc> rfc, pmem::obj::pool<root> pop, size_t pid, const char * opName, size_t param)
{
    rfc->announce_arr[pid]->op->param = param;
    rfc->announce_arr[pid]->op->name = opName;
    size_t opEpoch = rfc->announce_arr[pid]->res->epoch;
    size_t opVal = rfc->announce_arr[pid]->res->val;
	if (opVal != NONE and rfc->cEpoch >= opEpoch + 1) {
		return opVal;
	}
    size_t value = try_to_take_lock(rfc, opEpoch, pid);
	if (value != NONE){
		return value;
	}
	// std::cout << "in gc, combiner is: " << pid << std::endl;
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


int main(int argc, char *argv[]) {
	pmem::obj::pool<root> pop;
	pmem::obj::persistent_ptr<root> proot;

	const char* pool_file_name = "poolfile";
    size_t params [N];
    const char * ops [N];
    std::thread threads_pool[N];

    for (int pid=0; pid<N; pid++) {
            if (pid % 3 == 1) {
                params[pid] = NONE;
                ops[pid] = "Pop";
            }
            else {
                params[pid] = pid;
                ops[pid] = "Push";
            }
        }

	if (is_file_exists(pool_file_name)) {
		// open a pmemobj pool
		pop = pool<root>::open(pool_file_name, "layout");
		proot = pop.root();

        for (int pid=0; pid<N; pid++) {
            threads_pool[pid] = std::thread (recover, proot->rfc, pop, pid, ops[pid], params[pid]);
        }
										      
		for (int pid=0; pid<N; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->rfc);
		
		
		for (int pid=0; pid<N; pid++) {
            threads_pool[pid] = std::thread (op, proot->rfc, pop, pid, ops[pid], params[pid]);
        }
										      
		for (int pid=0; pid<N; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->rfc);
	}
	else {
		// create a pmemobj pool
		pop = pool<root>::create(pool_file_name, "layout", PMEMOBJ_MIN_POOL);
		proot = pop.root();
    	transaction::run(pop, [&] {
        	// allocation
			proot->rfc = make_persistent<recoverable_fc>();
		});
        for (int pid=0; pid<N; pid++) {
            allocate_announce(proot->rfc, pop, pid);
        }
		allocate_nodes_pool(proot->rfc, pop);
		std::cout << "Finished allocating!" << std::endl;
		
		for (int pid=0; pid<N; pid++) {
            threads_pool[pid] = std::thread (op, proot->rfc, pop, pid, ops[pid], params[pid]);
        }

		// usleep(1);
		// kill(getpid(), SIGKILL);

		for (int pid=0; pid<N; pid++) {
			threads_pool[pid].join();
		}
		print_state(proot->rfc);
	}
	
}