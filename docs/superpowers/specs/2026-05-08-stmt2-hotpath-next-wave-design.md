# stmt2 bind-exec hotpath next-wave design

## Context

`WSColumnPreparedStatement` already moved the stmt2 bind-exec path onto a flatter columnar payload and removed the worst execute-time copies. The current client-only benchmark shows columnar ahead of row mode, but the remaining CPU profile still clusters around three areas:

1. variable-width UTF-8 work
2. fixed-width float/double conversion overhead
3. row-state staging and clear bookkeeping

This spec defines the next optimization wave for those three areas only.

## Goals

1. Reduce total **setter + addBatch()** CPU cost for the stmt2 columnar path.
2. Keep the stmt2 wire format unchanged.
3. Preserve JDBC setter overwrite semantics.
4. Cover the full row-setter type surface, not just the benchmark schema.
5. Keep row-count validation lightweight and execute-time only.

## Non-goals

1. Do not change `WSRowPreparedStatement`.
2. Do not change the stmt2 protocol layout.
3. Do not add cross-column row-count checks during intermediate binding.
4. Do not introduce a self-managed unpooled byte-array backend in this round.

## Chosen approach

We considered three options for string handling:

1. flush-side single-pass UTF-8 write only
2. flush-side write plus cached string lengths
3. setter-side UTF-8 pre-encode

The selected option is **setter-side UTF-8 pre-encode**. This accepts some wasted work when a string setter is overwritten before `addBatch()`, but it removes UTF-8 work from the row flush hot path and lets string-backed and bytes-backed variable-width fields converge to one shared runtime representation.

## High-level design

### 1. Row-state layout

`Stmt2CurrentRowState` moves to a more physical staged layout:

```java
byte[] fixed1Values;
short[] fixed2Values;
int[] fixed4Values;
long[] fixed8Values;

byte[][] varValues;
int[] varLengths;

long[] nonNullBits;
int[] touchedIndexes;
boolean rowDirty;
```

Key points:

1. `fixed4Values` stores raw `INT/UINT/FLOAT` bits.
2. `fixed8Values` stores raw `BIGINT/UBIGINT/DOUBLE/TIMESTAMP` values.
3. All runtime variable-width values use `varValues + varLengths`, regardless of whether they came from `String` or `byte[]`.
4. Bitmap state replaces `boolean[] currentNull/currentStaged` on the hot path.
5. A lightweight touched structure remains for `clear()` so reference slots can be nulled without sweeping every column.
6. `rowDirty` preserves the distinction between an untouched row and a valid row whose fields all flush as null.
7. At the per-slot level, `unset` and explicit `null` both flush as null; the design keeps the row-level distinction through `rowDirty`, not through a third per-slot state.

### 2. Setter behavior

`WSColumnPreparedStatement` will normalize values into the staged physical layout immediately:

1. `setString`, `setNString`, and `tbname` encode to UTF-8 once at setter time and write `byte[] + len` into `varValues/varLengths`.
2. `setBytes`, blob-backed, and bytes-backed variable-width setters write into the same `varValues/varLengths`.
3. `setFloat` stores raw `int` bits directly into `fixed4Values`.
4. `setDouble` stores raw `long` bits directly into `fixed8Values`.
5. `setTimestamp` stores the normalized epoch value directly into `fixed8Values`.
6. Overwrite semantics remain unchanged: the latest setter wins until `addBatch()` snapshots the row.

This means the row-state is already in flush-ready form before `addBatch()`.

### 3. Variable-width runtime unification

With setter-side UTF-8 pre-encode, the runtime no longer needs separate string and bytes flush behavior.

The design therefore keeps two **source-level** concepts:

1. string-backed setters
2. bytes-backed setters

but collapses them into one **runtime** path:

1. shared staged storage: `varValues + varLengths`
2. shared flush loop: `flushEncodedVar(...)`
3. shared buffer append helper: `appendEncodedVar(byte[] value, int len)`

`tbname` remains a normal variable-width column in serialization. Its only extra responsibility is `table_count` derivation, which will compare already-encoded staged bytes instead of relying on a dedicated string slot.

### 4. Compiled flush plan

`Stmt2RowFlushPlan` remains prepare-time compiled, but the lane model is simplified around the new staged representation:

1. `ENCODED_VAR`
2. `FIXED1`
3. `FIXED2`
4. `FIXED4_RAW`
5. `FIXED8_RAW`

Runtime `flushRow()` will execute these compiled loops only. It will not branch between string-vs-bytes handling for variable-width fields anymore.

### 5. Column buffer append changes

`Stmt2ColumnFieldBuffer` gains shared runtime helpers aligned to the new staged layout:

1. `appendEncodedVar(byte[] value, int len)`
2. raw fixed-width append helpers for values already in on-wire bit form

For fixed-width numeric hot paths, the goal is to avoid value round-trips such as:

1. `int bits -> float -> int bits`
2. `long bits -> double -> long bits`

Null serialization remains unchanged and still emits stmt2-compatible `is_null[num]` bytes.

## Error handling and validation

The current validation strategy stays intentionally conservative:

1. `tbname` missing or empty still fails during row flush.
2. Cross-column row-count validation still happens once before execute.
3. No new mid-binding consistency checks are added.

This keeps the correctness model stable while reducing hot-path work.

## Type coverage

This design applies to the full row-setter stmt2 surface currently supported by the columnar path:

| Runtime lane | Covered physical types |
| --- | --- |
| `FIXED1` | `BOOL`, `TINYINT`, `UTINYINT` |
| `FIXED2` | `SMALLINT`, `USMALLINT` |
| `FIXED4_RAW` | `INT`, `UINT`, `FLOAT` |
| `FIXED8_RAW` | `BIGINT`, `UBIGINT`, `DOUBLE`, `TIMESTAMP` |
| `ENCODED_VAR` | `tbname`, `VARCHAR`, `NCHAR`, `JSON`, `VARBINARY`, `GEOMETRY`, `BLOB`, and existing bytes-backed logical forms |

Higher-level setters continue to map through the existing normalization layer:

1. `Date` / `Time` / `Timestamp` -> `TIMESTAMP`
2. `BigDecimal` -> current decimal bytes representation
3. `Blob` / blob stream -> encoded variable-width bytes
4. `setObject(...)` -> existing concrete setter matrix

## Testing plan

### Unit coverage

Update and extend:

1. `Stmt2CurrentRowStateTest`
2. `Stmt2ColumnBatchStateTest`
3. `WSColumnPreparedStatementTest`
4. `Stmt2ColumnFieldBuffer` serializer-level tests

New assertions should cover:

1. string-backed and bytes-backed setters converging to the same staged variable-width representation
2. raw fixed-width staging for float/double/timestamp
3. bitmap + touched-index clear behavior
4. `tbname` table-count derivation from staged bytes

### Performance validation

Primary benchmark:

1. `WsStmt2SerializationPerfCompareTest.benchmarkClientOnly_realisticWorkload_columnarVsRowMode`

Validation criteria:

1. total columnar **setter + addBatch()** time improves against the current implementation
2. current variable-width and string-path hotspots materially shrink in the post-change profile
3. float/double conversion hotspots disappear from the fixed-width flush path
4. `buildPayloadBuffer()` remains a non-goal and is not used as the success metric

## Expected outcome

The expected gains are:

1. less runtime branching inside variable-width row flush
2. no float/double bit reinterpretation churn during flush
3. lower row-state clear and boolean-array bookkeeping cost

Because strings are pre-encoded at setter time, some CPU moves earlier in the pipeline. Success is therefore measured by the combined `setter + addBatch()` cost, not by `flushRow()` alone.

## Deferred alternative

We explicitly defer the more aggressive `AutoExpandingBuffer` rewrite to a self-managed byte backend. The concern is not feasibility but pooling behavior and implementation risk: a plain self-managed `byte[]` path would not automatically inherit Netty pooling. If we revisit that direction later, it should be evaluated as a separate design around pooled slab management rather than folded into this optimization wave.
