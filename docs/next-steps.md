# Ideas

## Extract a sink for dispatch

One function counts events, hashes them, updates a toy LOB, and prints
every Nth via globals. Make the sink an abstract concept and compose
through a tee. New observer = a thing on the side.

## Pipeline as a library

Source, k-way merge, the merger-thread body, and the tree builder all
live as static inside main. Move to a shared header with a hierarchy
builder. Multi-level trees become testable on synthetic streams.

## Replace bool + sentinel with an enum

The parser returns nullptr on error and encodes "parsed but no
timestamp" as zero in the key. That's a state machine
(error / drop / ok) hidden behind two magic values. Pull the drop
policy fully into the parser; the caller gets just the enum.

## ChunkSpec — done

The splitter was private to main, tests couldn't reach it. Pulled into
its own header.

## Order if HW2 happens

Chunk → Pipeline → Sink → Enum. Each stands alone; pulling the
pipeline out makes the sink cheaper.
