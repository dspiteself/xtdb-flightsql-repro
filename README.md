# XTDB 2.2.0-beta3 FlightSQL repros

Standalone reproductions of three XTDB FlightSQL/ADBC Arrow-stream bugs.
They were found on XTDB `2.2.0-beta3` (Arrow Java 19.0.0), and the code involved is unchanged on `main` as of 2026-10-18.

Bugs 1 and 2 share one root cause, in `Xtdb.Connection`'s `ResultCursor.toArrowReader()`.
Bug 3 is separate.

Upstream PR with the fix for bugs 1 and 2: see the link at the bottom of this file.

## Running

The only dependency is `com.xtdb/xtdb-core 2.2.0-beta3` (see `deps.edn`).
Run every script from the repo root, on JDK 21+ with the Clojure CLI installed:

```sh
java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp "$(clojure -Spath)" clojure.main <file>.clj
```

Each script starts its own embedded in-memory node(s), prints its result, and exits.
XTDB's INFO log lines are interleaved with the output; the lines quoted below are the ones that matter.
`--add-opens=java.base/java.nio=ALL-UNNAMED` is required by Arrow; `--sun-misc-unsafe-memory-access=allow` silences a JDK 24+ warning and is not recognised before JDK 23, so drop it there.
The outputs below were captured on OpenJDK 25.

| File | Bug | What it shows |
| --- | --- | --- |
| `repro-minimal.clj` | 1 | A block-flushed struct field missing from the live row: FlightSQL fails with "no more field nodes". |
| `perm-jvm.clj` | 2 | Two nodes in one JVM, same struct fields in different orders: the client JVM SIGSEGVs. |
| `repro-segv-all.clj` | 2 | All 24 key orders of a 4-field struct on one node: 23 of them come back wrong over FlightSQL, silently. |
| `streamrace3.clj` + `stress-data.clj` | 3 | Concurrent Arrow streams in one JVM corrupt each other via Arrow's shared empty `ArrowBuf`. |
| `proof.clj` | 1 | Drives a real `ResultCursor` with the current page load and the proposed fix, across 6 data shapes x 4 queries, and compares with `xt/q`. |
| `proof-twodb.clj` | 2 | The same comparison for two databases with the same struct fields in different orders. |
| `xtdb-toArrowReader-conform-page.patch` | 1, 2 | The proposed fix, as a diff of `Xtdb.kt` at `v2.2.0-beta3`. |

## Bug 1: "no more buffers / no more field nodes" once a page's shape differs from the declared result type

### Repro

`repro-minimal.clj` starts a node with `:indexer {:rows-per-block 20}`.
It writes 50 rows whose struct `vq` has `{value, unit}`, so they flush to blocks.
Then it writes one live row whose `vq` has only `{value}`.

Expected output:

```
node    : [{:xt/id live, :vq {:value 2.0}}]
flight  : no more field nodes for field unit: Utf8 and vector []
```

The node returns the row; the same query over FlightSQL fails server-side.

`proof.clj` widens the scope to 6 data shapes x 4 queries (by-id live row, by-id flushed row, `SELECT *`, `FOR ALL SYSTEM_TIME`).
The current page load fails 21 of the 24 queries; both variants of the fix match `xt/q` in all 24 (48/48 `equal to xt/q: true`).
The failing shapes, each with flushed rows on one side and live rows on the other:

- a struct field present only in flushed rows;
- a scalar that is long in flushed rows and string in live rows - this fails even for by-id reads of the (all-long) flushed rows, because the page lacks the union's utf8 leg;
- a top-level column present only in flushed rows;
- a struct inside a list inside a struct;
- a struct in flushed rows and a string in live rows;
- a list of longs in flushed rows and a mixed list in live rows.

### Root cause

The FlightSQL producer (`xtdb.flight_sql.XtdbProducer`) streams the `ArrowReader` from `Xtdb.Statement.executeQuery()`.
That reader is `ResultCursor.toArrowReader()` in `core/src/main/kotlin/xtdb/api/Xtdb.kt` (line 1116 at `v2.2.0-beta3`):

```kotlin
val schema = Schema(resultTypes.map { (name, type) -> type.toField(name).withUnionTypeIds() })
...
override fun loadNextBatch(): Boolean =
    tryAdvance { inRel ->
        inRel.openDirectSlice(this.allocator).use { rel ->
            val loader = VectorLoader(this.vectorSchemaRoot)
            rel.openArrowRecordBatch().use { rb -> loader.load(rb) }
        }
    }
```

The root is built from the cursor's declared result type: the merged type across all the data the query can touch.
Each page, however, only carries the struct fields and union legs its own rows use.
A page from the live index is built from the live rows; a page from a flushed block is built from that block's column types.
Arrow's `VectorLoader` walks the root's schema and consumes field nodes and buffers one-for-one, by position.
When a page lacks a field or a leg, the loader runs out and throws `no more buffers for field X` or `no more field nodes for field X`, depending on which counter runs out first.

Server-side stack:

```
java.lang.IllegalArgumentException: no more field nodes for field unit: Utf8 and vector []
  at org.apache.arrow.vector.VectorLoader.loadBuffers(VectorLoader.java:109)
  at org.apache.arrow.vector.VectorLoader.load(VectorLoader.java:88)
  at xtdb.api.Xtdb$Connection$toArrowReader$1.loadNextBatch$lambda$0(Xtdb.kt:1127)
  at xtdb.operator.ProjectCursor.tryAdvance ...
  at xtdb.operator.scan.ScanCursor.tryAdvance(ScanCursor.kt:96)
  ...
  at xtdb.api.Xtdb$Connection$toArrowReader$1.loadNextBatch(Xtdb.kt:1123)
```

pgwire and in-process `xt/q` are unaffected: both read each page's `RelationReader` row by row, by name, and never force a page into a fixed Arrow schema.
Only Arrow consumers of `Xtdb.Statement.executeQuery().reader` hit this - FlightSQL, and ADBC clients that read the Arrow reader (Arrow's own `adbc-driver-flight-sql` fails identically).

Data that never crosses a block boundary has one shape, so its pages always match the declared type; that is why small test datasets pass.

### Fix

Conform each page to the declared schema before unloading it: copy its rows into a `Relation(allocator, schema)` with the page's row copier.
The row copier matches columns and struct children by name, fills absent struct fields with nulls, and widens values into the declared union legs.

```diff
                 override fun loadNextBatch(): Boolean =
                     tryAdvance { inRel ->
-                        inRel.openDirectSlice(this.allocator).use { rel ->
+                        Relation(this.allocator, schema).use { rel ->
+                            inRel.rowCopier(rel).copyRange(0, inRel.rowCount)
                             val loader = VectorLoader(this.vectorSchemaRoot)
                             rel.openArrowRecordBatch().use { rb ->
                                 loader.load(rb)
```

`proof.clj` reimplements this loop in Clojure over a real `ResultCursor`:
`:buggy` is the current load, `:fixed` copies via `openDirectSlice` then `rowCopier`, and `:fixed-direct` copies straight from the page as in the patch.

## Bug 2: wrong struct values (and a client JVM SIGSEGV) when a struct's field order differs from the declared type

### Repro

`perm-jvm.clj` runs two separate embedded nodes, one after the other, in one JVM.
Node 1 writes a string-keyed struct in key order `system, unit, value, code` into `first_table`.
Node 2 writes the same fields in order `value, unit, system, code` into `other_table`, and reads it with different SQL text.

Expected output (deterministic):

```
first_table node  : {:system "SYSTEM", :unit "UNIT", :value 60.0, :code "CODE"}
first_table flight: {:system "SYSTEM", :unit "UNIT", :value 60.0, :code "CODE"}
other_table node  : {:value 60.0, :unit "UNIT", :system "SYSTEM", :code "CODE"}
#
# A fatal error has been detected by the Java Runtime Environment:
#
#  SIGSEGV (0xb) ...
```

The JVM exits with status 134 and leaves an `hs_err_pid*.log`.

`repro-segv-all.clj` is the silent variant: one node, 24 tables, one per key order of `{value, unit, system, code}`.
The first order (`p00-vusc`) reads back correctly over FlightSQL; the other 23 do not, while the node itself returns the right values for all 24.
For example:

```
p01-vucs     node  : {:value 60.0, :unit "/min", :code "/min", :system "http://unitsofmeasure.org"}
p01-vucs     flight: {:value 60.0, :unit "/min", :system "/min", :code "http://unitsofmeasure.org"}
p06-uvsc     node  : {:unit "/min", :value 60.0, :system "http://unitsofmeasure.org", :code "/min"}
p06-uvsc     flight: {:value 8.487983164E-314, :unit "", :system "http://unitsofmeasure.org", :code "/min"}
```

No error is raised for any of them.

Each order alone, in a fresh JVM, reads back correctly.

### Root cause

This is the same positional `VectorLoader` load as bug 1.
When a page's struct children come in a different order from the declared struct, but the buffer counts are the same, `VectorLoader` does not throw: each declared child silently takes the buffers of whichever page child sits at its position.
For example, the f64 `value` buffer gets used as a Utf8 offsets buffer.

Why the declared order can differ from the data's order, even across nodes:

- `xtdb.arrow.VectorType.Struct` is a Kotlin data class over `children: Map<String, VectorType>`, so its `equals`/`hashCode` ignore field order: structs with fields `(system value)` and `(value system)` are equal, with equal hash codes.
- JVM-global memo caches key on those types.
  The likely one is `xtdb.expression/emit-projection`'s `util/lru-memoize` over `[expr opts]`, where `opts` carries the input vector types; it returns a `:return-type` with the first caller's field order.
  (Which cache supplies the stale type was not pinned down.)
- So the declared result type keeps the first-seen field order for that field set, JVM-wide, while each page has its own data's order.

The crash comes from the stock `xtdb.flight-sql` Clojure client, which decodes Utf8 without bounds checks:
`ExtensibleBuffer.toByteBuffer` builds its view as `MemorySegment.ofAddress(memoryAddress() + start).reinterpret(len).asByteBuffer()`, and `VariableWidthVector.getByteArray` reads `[start, start+len)` straight from the bogus offsets.
f64 bits read as offsets (0x404E0000, the high word of 60.0, is 1078853632) become a ~1 GB read off the end of the mapping:

```
Unsafe.copyMemory0 <- ByteBuffer.get <- xtdb.arrow.VariableWidthVector.getByteArray <- Utf8Vector.getObject0
  <- StructVector.getObject0 <- RelationReader.toMaps <- xtdb.adbc/reduce-page <- xtdb.flight-sql/reduce-stream <- xt/q
```

Arrow's own `adbc-driver-flight-sql` bounds-checks and throws `index: 0, length: 1078853632 (expected: range(0, 0))` instead of crashing; orders that only swap same-typed fields come back silently wrong there too.

pgwire and `xt/q` read pages by name, so they are unaffected.

### Fix

The bug 1 fix fixes this too: the row copier matches struct children by name, so the page's order no longer matters.
`proof-twodb.clj` attaches two databases on one node, writes the same struct fields in different orders, and compares each load with pgwire:

```sh
MODES=fixed,fixed-direct java ... clojure.main proof-twodb.clj
```

prints `equal to pgwire: true` for both databases.
With the default `MODES` (which runs `buggy` first) the JVM SIGSEGVs on database `b`.

Two follow-ups for XTDB, not part of the PR:

- Struct type equality (or the memo cache keys) should be order-sensitive, or struct field order canonicalised, so the declared result type matches the data.
- The client decoder should not trust wire offsets: bounds-check `toByteBuffer`, or validate received batches.

## Bug 3: concurrent Arrow streams race on Arrow's shared empty `ArrowBuf`

### Repro

```sh
java ... clojure.main streamrace3.clj 15 8 false no-vsr   # 8 concurrent readers for 15 s
java ... clojure.main streamrace3.clj 15 1 false no-vsr   # control: 1 reader
```

Arguments: seconds, reader threads, whether each thread reuses one connection (`true`) or opens one per query (`false`), and the mode.
`no-vsr` checks every buffer of each unloaded batch the way `ArrowMessage.asInputStream` does.

The script emulates `XtdbProducer.streamArrowReader` in-process, with no gRPC and no FlightSQL client: `executeQuery().getReader()`, then `VectorUnloader` on the reader's root, then `nioBuffer()` per buffer, against a table with nested non-nullable columns.

Expected output (counts vary by machine):

```
readers 8 mode no-vsr reads 209419 bad 21279
   FAILING BUF capacity=0 writerIndex=4 refmgr= node-empty?=true
   class java.lang.IndexOutOfBoundsException index: 0, length: 4 (expected: range(0, 0))

readers 1 mode no-vsr reads 51116 bad 0
```

`node-empty?=true` means the failing buffer is `identical?` to `(.getEmpty (.getAllocator node))`.
With one reader there are no failures.
Over real FlightSQL the same race shows up as server-side `RST_STREAM CANCEL`s, rows with wrong values, and eventually a client SIGSEGV.
The same load through `xt/q` or pgwire gives no bad reads, because those paths never set indices on Arrow roots.

### Root cause

Arrow's zero-length buffer is one object per JVM: with the netty allocation manager it is `NettyAllocationManager.EMPTY_BUFFER`, a `static final ArrowBuf` that `allocator.getEmpty()` returns for every allocator.
Its reader and writer indices are ordinary mutable fields.
XTDB puts it into Arrow batches routinely:

- `VariableWidthVector` / `StructVector.unloadPage` add `al.empty` as the validity buffer of every non-nullable vector;
- `ExtensibleBuffer(allocator)` starts from `allocator.empty`;
- `transferOwnership` of an empty buffer returns the same object.

Its indices are then written by XTDB's `ExtensibleBuffer.loadBuffer` (`writerIndex(n)`) and `unloadBuffer` (`readerIndex(0)`), and by Arrow's `setReaderAndWriterIndex` on load and unload.
So concurrent Arrow-streaming queries in one JVM overwrite each other's buffer lengths, and one stream serialises the empty buffer with a length another stream just set.

Arrow Java 19.0.0 is the newest release and has no fix for this.

### Fix (not in the PR)

Either in XTDB - never hand the shared empty buffer to code that sets its indices (emit a fresh zero-length buffer per unload, or skip `writerIndex` on empty buffers in `loadBuffer`) - or in Arrow, by making the empty buffer's index setters no-ops.

## Upstream

- Fix for bugs 1 and 2: https://github.com/xtdb/xtdb/pulls?q=author%3Adspiteself
