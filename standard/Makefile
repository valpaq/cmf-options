# standard task — single-file NDJSON ingestion.
#
# Targets:
#   make            -> ingest_opt + ingest_fast
#   make opt        -> ingest_opt  (simdjson variant)
#   make fast       -> ingest_fast (custom schema-aware parser, no simdjson)
#   make run FILE=  -> ingest_opt on a single NDJSON
#   make run-fast FILE=
#
# simdjson is detected via pkg-config; falls back to plain -lsimdjson.

CXX       ?= clang++
CXXSTD    := -std=c++20
OPT       := -O3 -march=native -flto -DNDEBUG
WARN      := -Wall -Wextra -Wno-unused-parameter
INCLUDES  := -I../common $(shell pkg-config --cflags simdjson 2>/dev/null)
LIBS      := $(shell pkg-config --libs   simdjson 2>/dev/null)
ifeq ($(strip $(LIBS)),)
LIBS      := -lsimdjson
endif

CXXFLAGS  := $(CXXSTD) $(OPT) $(WARN) $(INCLUDES)
LDFLAGS   := $(LIBS)

COMMON_HDR := ../common/event.h ../common/parse.h ../common/fast_parse.h
SRC_OPT   := src/main.cpp
SRC_FAST  := src/main_fast.cpp
HDR_OPT   := src/parse_simdjson.h $(COMMON_HDR)
HDR_FAST  := $(COMMON_HDR)
BIN_OPT   := ingest_opt
BIN_FAST  := ingest_fast

.PHONY: all clean run run-fast rebuild opt fast

all: $(BIN_OPT) $(BIN_FAST)

opt:  $(BIN_OPT)
fast: $(BIN_FAST)

$(BIN_OPT): $(SRC_OPT) $(HDR_OPT)
	$(CXX) $(CXXFLAGS) $(SRC_OPT) -o $(BIN_OPT) $(LDFLAGS)

$(BIN_FAST): $(SRC_FAST) $(HDR_FAST)
	$(CXX) $(CXXFLAGS) $(SRC_FAST) -o $(BIN_FAST)

clean:
	rm -f $(BIN_OPT) $(BIN_FAST)

rebuild: clean all

# Usage: make run FILE=/path/to/file.ndjson
run: $(BIN_OPT)
	@if [ -z "$(FILE)" ]; then echo "Usage: make run FILE=/path/to/file.ndjson"; exit 1; fi
	./$(BIN_OPT) $(FILE)

run-fast: $(BIN_FAST)
	@if [ -z "$(FILE)" ]; then echo "Usage: make run-fast FILE=/path/to/file.ndjson"; exit 1; fi
	./$(BIN_FAST) $(FILE)
