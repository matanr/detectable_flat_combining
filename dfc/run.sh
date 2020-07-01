#!/bin/bash
# Create a Makefile with CMake, compile and run DFC

make clean ; rm -rf CMakeCache.txt CMakeFiles/ Makefile dfc /dev/shm/dfc_shared ; cmake . ; make ; ./dfc
