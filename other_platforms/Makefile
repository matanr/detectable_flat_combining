CXX = g++  #-8
# CXXFLAGS = -std=c++14 -g -O2 -DPWB_IS_CLFLUSHOPT
CXXFLAGS = -std=c++14 -g -O2 -DPWB_IS_CLFLUSH
# CXXFLAGS = -std=c++14 -g -O2 -DPWB_IS_NOP
# Use these options for castor-1 (Optane DC PM):
#CXXFLAGS = -std=c++14 -g -O2 -DPWB_IS_CLWB -DPM_REGION_SIZE=4*1024*1024*1024ULL -DPM_FILE_NAME="\"/mnt/pmem0/durable\""    
#CXXFLAGS += -fuse-ld=gold  -fsanitize=address
# Possible options for PWB are:
# -DPWB_IS_CLFLUSH		pwb is a CLFLUSH and pfence/psync are nops
# -DPWB_IS_CLFLUSHOPT	pwb is a CLFLUSHOPT and pfence/psync are SFENCE 
# -DPWB_IS_CLWB			pwb is a CLWB and pfence/psync are SFENCE
# -DPWB_IS_NOP			pwb/pfence/psync are nops

INCLUDES = -I../

# This library is needed for PMDK
PMDKLIBS=-I/home/matanr/nvram/libpmemobj-cpp/include/ -L/usr/local/lib -lpmemobj


# PTMs for which we want to build concurrent benchmarks
CONC_PTM_NAMES := \
	bin-romlogfc \
	bin-oflf \
	# bin-pmdk \
	
	
BINARIES_SEQ = \
	$(subst bin-, bin/pstack-ll-,            $(CONC_PTM_NAMES)) \
#	$(subst bin-, bin/pstack-fat-,           $(CONC_PTM_NAMES)) \
	

all: $(BINARIES_SEQ) persistencyclean

.PHONY : clean persistencyclean

clean: persistencyclean
	rm -f bin/pstack*

persistencyclean:
	rm -f /mnt/pmem0/durable
	rm -f /dev/shm/*_shared
	rm -f /dev/shm/*_shared_main
	rm -f /dev/shm/*_shared_back
	rm -f /dev/shm/psegments/*
	rm -f /dev/shm/pmdk_shared
	rm -f /dev/shm/pmdk_shared_friedman	


#
# Multithreaded stack (linked list)
#
bin/pstack-ll-romlogfc: pstack-ll.cpp PBenchmarkStacks.hpp TMLinkedListStackByRef.hpp romulus/RomLogFC.hpp
	$(CXX) $(CXXFLAGS) -DUSE_ROM_LOG_FC $(INCLUDES) pstack-ll.cpp -o bin/pstack-ll-romlogfc -lpthread

# bin/pstack-ll-pmdk: pstack-ll.cpp PBenchmarkStacks.hpp TMLinkedListStackByRef.hpp pmdk/PMDKTM.hpp
# 	$(CXX) $(CXXFLAGS) -DUSE_PMDK $(INCLUDES) pstack-ll.cpp -o bin/pstack-ll-pmdk -lpthread -lpmemobj

bin/pstack-ll-oflf: pstack-ll.cpp PBenchmarkStacks.hpp TMLinkedListStackByRef.hpp one_file/OneFilePTMLF.hpp
	$(CXX) $(CXXFLAGS) -DUSE_OFLF $(INCLUDES) pstack-ll.cpp -o bin/pstack-ll-oflf -lpthread

