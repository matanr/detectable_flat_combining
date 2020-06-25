/*
 * Copyright 2017-2020
 *   Andreia Correia <andreia.veiga@unine.ch>
 *   Pedro Ramalhete <pramalhe@gmail.com>
 *   Pascal Felber <pascal.felber@unine.ch>
 *
 * This work is published under the MIT license. See LICENSE.txt
 */
#ifndef _PERSISTENT_BENCHMARK_STACKS_H_
#define _PERSISTENT_BENCHMARK_STACKS_H_

#include <atomic>
#include <chrono>
#include <thread>
#if defined(COUNT_PWB)
#include <mutex>
#endif
#include <string>
#include <vector>
#include <algorithm>
#include <cassert>


using namespace std;
using namespace chrono;

#if defined(COUNT_PWB)
std::mutex pLock; // Used to add local PWB and PFENCE instructions count to the global variables
int pwbCounter = 0;
int pfenceCounter = 0;
int psyncCounter = 0;
#endif

#if defined(USE_ROM_LOG_FC) && defined(COUNT_PWB)
#include "romulus/RomLogFC.hpp"
using namespace romlogfc;
#elif defined(USE_OFLF) && defined(COUNT_PWB)
#include "one_file/OneFilePTMLF.hpp"
using namespace poflf;
#elif defined(USE_PMDK) && defined(COUNT_PWB)
// extern __thread uint64_t tl_num_pwbs;
extern thread_local uint64_t tl_num_pwbs;
#endif

struct UserData  {
    long long seq;
    int tid;
    UserData(long long lseq, int ltid) {
        this->seq = lseq;
        this->tid = ltid;
    }
    UserData() {
        this->seq = -2;
        this->tid = -2;
    }
    UserData(const UserData &other) : seq(other.seq), tid(other.tid) { }

    bool operator < (const UserData& other) const {
        return seq < other.seq;
    }
};


/**
 * This is a micro-benchmark for persistent queues
 */
class PBenchmarkStacks {

private:

    struct Result {
        nanoseconds nsEnq = 0ns;
        nanoseconds nsDeq = 0ns;
        long long numEnq = 0;
        long long numDeq = 0;
        long long totOpsSec = 0;

        Result() { }

        Result(const Result &other) {
            nsEnq = other.nsEnq;
            nsDeq = other.nsDeq;
            numEnq = other.numEnq;
            numDeq = other.numDeq;
            totOpsSec = other.totOpsSec;
        }

        bool operator < (const Result& other) const {
            return totOpsSec < other.totOpsSec;
        }
    };

    // Performance benchmark constants
    static const long long kNumPairsWarmup =     1000000LL;     // Each threads does 1M iterations as warmup

    // Contants for Ping-Pong performance benchmark
    static const int kPingPongBatch = 1000;            // Each thread starts by injecting 1k items in the queue


    static const long long NSEC_IN_SEC = 1000000000LL;

    const uint64_t kNumElements = 0; // Number of initial items in the stack

    int numThreads;

public:

    PBenchmarkStacks(int numThreads) {
        this->numThreads = numThreads;
    }


    /**
     * enqueue-dequeue pairs: in each iteration a thread executes an enqueue followed by a dequeue;
     * the benchmark executes 10^8 pairs partitioned evenly among all threads;
     */
    template<typename STACK, typename PTM>
    tuple<uint64_t, double, double> pushPop(std::string& className, const long numPairs, const int numRuns) {
        cout << "in push pop" << endl;
        nanoseconds deltas[numThreads][numRuns];
        atomic<bool> startFlag = { false };
        STACK* stack = nullptr;
        className = STACK::className();
        cout << "##### " << className << " #####  \n";

        auto pushpop_lambda = [this,&startFlag,&numPairs,&stack](nanoseconds *delta, const int tid) {
            //UserData* ud = new UserData{0,0};
            uint64_t* ud = new uint64_t(42);
            while (!startFlag.load()) {} // Spin until the startFlag is set
            // Measurement phase
            auto startBeats = steady_clock::now();
            for (long long iter = 0; iter < numPairs/numThreads; iter++) {
                stack->push(ud);
                if (stack->pop() == nullptr) cout << "Error at measurement pop() iter=" << iter << "\n";
            }
            auto stopBeats = steady_clock::now();
            *delta = stopBeats - startBeats;
            #if defined(COUNT_PWB) && (defined(USE_ROM_LOG_FC) || defined(USE_OFLF))
            std::lock_guard<std::mutex> lock(pLock);
            pwbCounter += localPwbCounter;
            pfenceCounter += localPfenceCounter;
            psyncCounter += localPsyncCounter;
            #elif defined(COUNT_PWB) && defined(USE_PMDK)
            std::lock_guard<std::mutex> lock(pLock);
            pwbCounter += tl_num_pwbs;
            #endif
        };

        auto randop_lambda = [this,&startFlag,&numPairs,&stack](nanoseconds *delta, const int tid) {
		uint64_t* ud = new uint64_t(42);
		while (!startFlag.load()) {} // Spin until the startFlag is set
		// Measurement phase
		auto startBeats = steady_clock::now();
		for (long long iter = 0; iter < 2 * numPairs/numThreads; iter++) {
			int randop = rand() % 2;         // randop in the range 0 to 1
			if (randop == 0) {
                stack->push(ud);
			}
			else if (randop == 1) {
                stack->pop();
			}
		}
		auto stopBeats = steady_clock::now();
		*delta = stopBeats - startBeats;
	};

        for (int irun = 0; irun < numRuns; irun++) {
            PTM::updateTx([&] () { // It's ok to capture by reference, only the main thread is active (but it is not ok for CX-PTM)
                stack = PTM::template tmNew<STACK>();
            });
            // Fill the queue with an initial amount of nodes
            uint64_t* ud = new uint64_t(41);
            for (uint64_t ielem = 0; ielem < kNumElements; ielem++) {
                PTM::updateTx([&] () {
                    stack->push(ud);
                });
            }
            thread enqdeqThreads[numThreads];
            // for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = thread(randop_lambda, &deltas[tid][irun], tid);
            for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid] = thread(pushpop_lambda, &deltas[tid][irun], tid);
            startFlag.store(true);
            // Sleep for 2 seconds just to let the threads see the startFlag
            this_thread::sleep_for(2s);
            for (int tid = 0; tid < numThreads; tid++) enqdeqThreads[tid].join();
            startFlag.store(false);
            PTM::updateTx([&] () {
                PTM::tmDelete(stack);
            });
        }

        // Sum up all the time deltas of all threads so we can find the median run
        vector<nanoseconds> agg(numRuns);
        for (int irun = 0; irun < numRuns; irun++) {
            agg[irun] = 0ns;
            for (int tid = 0; tid < numThreads; tid++) {
                agg[irun] += deltas[tid][irun];
            }
        }

        // Compute the median. numRuns should be an odd number
        sort(agg.begin(),agg.end());
        auto median = agg[numRuns/2].count()/numThreads; // Normalize back to per-thread time (mean of time for this run)

        cout << "Total Ops/sec = " << numPairs*2*NSEC_IN_SEC/median << "\n";

        #if defined(COUNT_PWB) && (defined(USE_ROM_LOG_FC) || defined(USE_OFLF))
        double pwbPerOp = double(pwbCounter) / double(numPairs*2);
		double pfencePerOp = double(pfenceCounter) / double(numPairs*2);
        double psyncPerOp = double(psyncCounter) / double(numPairs*2);
		cout << "#pwb/#op: " << fixed << pwbPerOp;
		cout << ", #pfence/#op: " << fixed << pfencePerOp;
        cout << ", #psync/#op: " << fixed << psyncPerOp << endl;

		pwbCounter = 0; pfenceCounter = 0; psyncCounter = 0; 
		localPwbCounter = 0; localPfenceCounter = 0; localPsyncCounter = 0;
        return make_tuple(numPairs*2*NSEC_IN_SEC/median, pwbPerOp, pfencePerOp);
        #elif defined(COUNT_PWB) && defined(USE_PMDK)
        double pwbPerOp = double(pwbCounter) / double(numPairs*2);
		cout << "#pwb/#op: " << fixed << pwbPerOp;

		pwbCounter = 0;
		tl_num_pwbs = 0;
        return make_tuple(numPairs*2*NSEC_IN_SEC/median, pwbPerOp, 0);
        #endif
        return make_tuple(numPairs*2*NSEC_IN_SEC/median, 0, 0);
    }

};

#endif
