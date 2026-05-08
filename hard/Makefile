# hard task — folder of ~20 NDJSON files merged via flat / hierarchy strategies.
#
# Targets:
#   make                    -> ingest_hard
#   make run FOLDER= MODE=  -> ./ingest_hard <folder> [flat|hierarchy|both]
#
# Uses only the schema-aware parser in ../common/fast_parse.h — simdjson
# is not required for this binary.

CXX       ?= clang++
CXXSTD    := -std=c++20
OPT       := -O3 -march=native -flto -DNDEBUG
WARN      := -Wall -Wextra -Wno-unused-parameter
INCLUDES  := -I../common

CXXFLAGS  := $(CXXSTD) $(OPT) $(WARN) $(INCLUDES)
PTHREAD   := -pthread

COMMON_HDR := ../common/event.h ../common/parse.h ../common/fast_parse.h
SRC_HARD  := src/main_hard.cpp
HDR       := src/spsc_ring.h src/tournament.h $(COMMON_HDR)
BIN_HARD  := ingest_hard

.PHONY: all clean run rebuild

all: $(BIN_HARD)

$(BIN_HARD): $(SRC_HARD) $(HDR)
	$(CXX) $(CXXFLAGS) $(SRC_HARD) -o $(BIN_HARD) $(PTHREAD)

clean:
	rm -f $(BIN_HARD)

rebuild: clean all

# Usage: make run FOLDER=/path/to/dir [MODE=flat|hierarchy|both]
run: $(BIN_HARD)
	@if [ -z "$(FOLDER)" ]; then echo "Usage: make run FOLDER=/path/to/dir [MODE=flat|hierarchy|both]"; exit 1; fi
	./$(BIN_HARD) $(FOLDER) $(if $(MODE),$(MODE),both)
