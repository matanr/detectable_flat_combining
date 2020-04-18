/******************************************************************************
 * Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Concurrency Freaks nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************
 */

#ifndef _TREIBER_STACK_HP_H_
#define _TREIBER_STACK_HP_H_

#include <atomic>
#include <stdexcept>
#include "HazardPointers.hpp"


/**
 * <h1> R. Kent Treiber's Stack </h1>
 *
 * The lock-free stack designed by R. K Treiber
 *
 * push algorithm: Treiber
 * pop algorithm: Treiber
 * Consistency: Linearizable
 * push() progress: lock-free
 * pop() progress: lock-free
 * Memory unbounded: singly-linked list based
 * Memory Reclamation: Hazard Pointers (lock-free), only for pop()
 *
 * Link to paper:
 * "Systems programming: Coping with parallelism. Technical Report RJ 511"
 * http://domino.research.ibm.com/library/cyberdig.nsf/papers/58319A2ED2B1078985257003004617EF/$File/rj5118.pdf
 *
 * <p>
 * The paper on Hazard Pointers is named "Hazard Pointers: Safe Memory
 * Reclamation for Lock-Free objects" and it is available here:
 * http://web.cecs.pdx.edu/~walpole/class/cs510/papers/11.pdf
 *
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
template<typename T>
class TreiberStack {

private:
    struct Node {
        T* item;
        Node* next;

        Node(T* item, Node* lhead) : item{item}, next{lhead} { }
    };

    bool casHead(Node *cmp, Node *val) {
        return head.compare_exchange_strong(cmp, val);
    }

    alignas(128) std::atomic<Node*> head;

    static const int MAX_THREADS = 128;
    const int maxThreads;

    Node* sentinel = new Node(nullptr, nullptr);

    // We need one hazard pointer for pop()
    HazardPointers<Node> hp {1, maxThreads};
    const int kHpHead = 0;


public:
    TreiberStack(int maxThreads=MAX_THREADS) : maxThreads{maxThreads} {
        head.store(sentinel, std::memory_order_relaxed);
    }


    ~TreiberStack() {
        while (pop(0) != nullptr); // Drain the stack
        delete head.load();        // Delete the last node
    }


    std::string className() { return "TreiberStack"; }


    bool push(T* item, const int tid) {
        if (item == nullptr) throw std::invalid_argument("item can not be nullptr");
        Node* lhead = head.load();
        Node* newNode = new Node(item, lhead);
        while (!head.compare_exchange_weak(lhead, newNode)) { // lhead gets updated in case of failure
            newNode->next = lhead;
        }
        return true;
    }


    T* pop(const int tid) {
        T* item = nullptr;
        while (true) {
            Node* lhead = hp.set(kHpHead, head.load(), tid);
            if (lhead == sentinel) break; // stack is empty
            if (lhead != head.load()) continue;
            if (head.compare_exchange_weak(lhead, lhead->next.load())) {
                item = lhead->item;
                hp.retire(lhead, tid);
                break;
            }
        }
        hp.clear(tid);
        return item;
    }
};

#endif /* _TREIBER_STACK_HP_H_ */
