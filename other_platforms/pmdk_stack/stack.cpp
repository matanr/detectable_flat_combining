// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2015-2018, Intel Corporation */

/*
 * stack.cpp -- stack example implemented using pmemobj cpp bindings
 *
 * Please see pmem.io blog posts for more details.
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj_cpp_examples_common.hpp>
#include <stdexcept>
#include <string>
#include <sys/stat.h>

#define LAYOUT "stack"

namespace
{

/* available stack operations */
enum stack_op {
	UNKNOWN_STACK_OP,
	STACK_PUSH,
	STACK_POP,
	STACK_SHOW,

	MAX_STACK_OP,
};

/* stack operations strings */
const char *ops_str[MAX_STACK_OP] = {"", "push", "pop", "show"};

/*
 * parse_stack_op -- parses the operation string and returns matching stack_op
 */
stack_op
parse_stack_op(const char *str)
{
	for (int i = 0; i < MAX_STACK_OP; ++i)
		if (strcmp(str, ops_str[i]) == 0)
			return (stack_op)i;

	return UNKNOWN_STACK_OP;
}
}

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::pool_base;
using pmem::obj::transaction;

namespace examples
{

/*
 * Persistent memory list-based stack
 *
 * A simple, not template based, implementation of stack using
 * libpmemobj C++ API. It demonstrates the basic features of persistent_ptr<>
 * and p<> classes.
 */
class pmem_stack {

	/* entry in the list */
	struct pmem_entry {
		persistent_ptr<pmem_entry> next;
		p<uint64_t> value;
	};

public:
	/*
	 * Inserts a new element at the top of the stack.
	 */
	void
	push(pool_base &pop, uint64_t value)
	{
		transaction::run(pop, [&] {
			auto n = make_persistent<pmem_entry>();

			n->value = value;
			n->next = head;
			head = n;
		});
	}

	/*
	 * Removes the top element in the stack.
	 */
	uint64_t
	pop(pool_base &pop)
	{
		uint64_t ret = 0;
		transaction::run(pop, [&] {
			if (head == nullptr)
				transaction::abort(EINVAL);

			ret = head->value;
			auto n = head->next;

			delete_persistent<pmem_entry>(head);
			head = n;
		});
		return ret;
	}

	/*
	 * Prints the entire contents of the stack.
	 */
	void
	show(void) const
	{
		for (auto n = head; n != nullptr; n = n->next)
			std::cout << n->value << std::endl;
	}

private:
	persistent_ptr<pmem_entry> head;
};

} /* namespace examples */

int
main(int argc, char *argv[])
{
	if (argc < 3) {
		std::cerr << "usage: " << argv[0]
			  << " file-name [push [value]|pop|show]" << std::endl;
		return 1;
	}

	const char *path = argv[1];

	stack_op op = parse_stack_op(argv[2]);

	pool<examples::pmem_stack> pop;

	if (file_exists(path) != 0) {
		pop = pool<examples::pmem_stack>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
	} else {
		pop = pool<examples::pmem_stack>::open(path, LAYOUT);
	}

	auto q = pop.root();
	switch (op) {
		case STACK_PUSH:
			q->push(pop, std::stoull(argv[3]));
			break;
		case STACK_POP:
			std::cout << q->pop(pop) << std::endl;
			break;
		case STACK_SHOW:
			q->show();
			break;
		default:
			throw std::invalid_argument("invalid stack operation");
	}

	pop.close();

	return 0;
}
