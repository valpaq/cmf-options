# Top-level — delegates to standard/ and hard/ Makefiles.
#
# Build targets:
#   make            -> builds standard binaries (ingest_opt + ingest_fast)
#                      and hard binary (ingest_hard)
#   make standard   -> just standard/
#   make hard       -> just hard/
#   make clean      -> clean both
#   make rebuild    -> clean + all
#
# Test targets:
#   make test       -> unit tests (76 assertions)
#   make smoke      -> black-box smoke (empty file / empty folder)
#
# Run targets (DATA / FILE override the defaults below):
#   make run-fast   -> ./standard/ingest_fast $(FILE)
#   make run-opt    -> ./standard/ingest_opt  $(FILE)
#   make run-hard   -> ./hard/ingest_hard $(DATA) $(MODE)
#   make bench      -> ./bench.sh $(DATA)
#
# Examples:
#   make run-hard
#   make run-hard MODE=flat
#   make run-fast FILE=path/to/some.mbo.json
#   make run-hard DATA=/abs/path/to/folder PRINT_EVERY=10000000

DATA ?= ./data
FILE ?= $(firstword $(wildcard $(DATA)/*.mbo.json))
MODE ?= both

.PHONY: all standard hard test smoke clean rebuild \
        run-fast run-opt run-hard bench

all: standard hard

standard:
	$(MAKE) -C standard

hard:
	$(MAKE) -C hard

test:
	$(MAKE) -C tests run

smoke: all
	$(MAKE) -C tests smoke

clean:
	$(MAKE) -C standard clean
	$(MAKE) -C hard clean
	$(MAKE) -C tests clean

rebuild: clean all

run-fast: standard
	@test -n "$(FILE)" || { echo "no FILE — set DATA=<folder> or FILE=<path>"; exit 2; }
	./standard/ingest_fast $(FILE)

run-opt: standard
	@test -n "$(FILE)" || { echo "no FILE — set DATA=<folder> or FILE=<path>"; exit 2; }
	./standard/ingest_opt $(FILE)

run-hard: hard
	@test -d "$(DATA)" || { echo "DATA=$(DATA) is not a directory"; exit 2; }
	./hard/ingest_hard $(DATA) $(MODE)

bench: all
	@test -d "$(DATA)" || { echo "DATA=$(DATA) is not a directory"; exit 2; }
	./bench.sh $(DATA)
