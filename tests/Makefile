CXX      ?= clang++
CXXFLAGS := -std=c++20 -O2 -g -Wall -Wextra -Wno-unused-parameter
PTHREAD  := -pthread

BIN  := test_main
SRC  := test_main.cpp
DEPS := ../common/event.h ../common/parse.h ../common/fast_parse.h \
        ../hard/src/chunk.h ../hard/src/spsc_ring.h ../hard/src/tournament.h

.PHONY: all run smoke clean

all: $(BIN)

$(BIN): $(SRC) $(DEPS)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(BIN) $(PTHREAD)

run: $(BIN)
	./$(BIN)

# Black-box smoke: requires both binaries to be built.
smoke:
	@$(MAKE) -C ../standard >/dev/null
	@$(MAKE) -C ../hard     >/dev/null
	./smoke.sh

clean:
	rm -f $(BIN)
