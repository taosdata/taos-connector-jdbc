# stmt2 fast mode simplification design

## Problem

The current `WSColumnFastPreparedStatement` fast path is functionally correct, but it regressed performance because the implementation kept too much structure from the compatibility path: extra helper layers, callback-style append wrappers, per-write bookkeeping, and a separate `Stmt2WriteOnceBatchState` object. Arthas profiling showed the slowdown is in the setter hot path, not in `addBatch()` or request assembly.

The goal of this redesign is to make fast mode truly hot-path oriented: fewer objects, fewer method layers, fewer generic abstractions, and validation only at execution time.

## Goals

1. Keep `stmt2BindMode=fast` as the default stmt2 bind-exec path.
2. Make `setXxx()` write directly into per-column buffers.
3. Remove row-state, duplicate-set tracking, snapshot/rollback logic, and other setter-time bookkeeping from fast mode.
4. Validate row-count consistency only at execution boundaries.
5. Correct unsigned-type handling to match the existing JDBC parameter binding behavior.

## Non-goals

1. Preserve JDBC overwrite semantics inside fast mode.
2. Change routing for compatibility column mode or line mode.
3. Change stmt2 wire format or bind-exec request shape.
4. Add new compatibility fallbacks inside fast mode.

## Approved design

### 1. Object structure

`Stmt2WriteOnceBatchState` will be removed.

`WSColumnFastPreparedStatement` will directly own:

- `Stmt2FieldMeta[] fieldMetas`
- `byte[] fixedWidths`
- `Stmt2ColumnFieldBuffer[] columnBuffers`
- `Stmt2ColumnFieldBuffer.BufferSizeHints[] bufferSizeHints`
- `int expectedRowCount`
- `int tbNameFieldIdx`

Fast-mode-specific operations such as reset, row-count validation, payload build, and close-time release will live directly in `WSColumnFastPreparedStatement`.

### 2. Setter behavior

All `setXxx()` methods write directly to the target `Stmt2ColumnFieldBuffer`.

Fast mode will not keep:

- current-row cache/state
- per-row touched flags
- duplicate-set detection
- rollback snapshots
- generic `ColumnAppender` callback wrappers

The implementation should prefer direct calls such as `columnBuffers[idx].appendInt(...)`, `appendTimestamp(...)`, `appendString(...)`, `appendEncodedVar(...)`, and `appendNull()`.

The intent is to keep each setter as close as possible to:

1. resolve field/type/index
2. perform required value-range validation
3. append directly to the buffer

### 3. Batch and execute semantics

#### `addBatch()`

`addBatch()` does not flush a staged row. It only increments `expectedRowCount`.

#### `executeBatch()`

Before sending, fast mode validates that every column buffer has exactly `expectedRowCount` rows.

If any column has a different row count:

1. reset all fast-mode buffers and counters
2. throw `SQLException`

#### `executeUpdate()` and `execute()`

Direct execute does not require `addBatch()`.

For direct execute, fast mode requires that **all columns contain exactly one row**.

If any column is not exactly one row:

1. reset all fast-mode buffers and counters
2. throw `SQLException`

This preserves the expected single-row semantics of `executeUpdate()` while still using direct-append setters.

#### `clearBatch()`

`clearBatch()` resets all fast-mode buffers, size hints, and counters.

#### `clearParameters()`

`clearParameters()` remains unsupported in fast mode.

### 4. `tbname` handling

`tbname` will no longer have special row-state handling.

It will be treated like a string/var-width field with one extra rule:

- table name cannot be null or empty

After that validation, it follows the same direct append path as string data. Table count continues to be derived from the `tbname` column buffer at payload-build time.

### 5. Unsigned type handling

Unsigned-type validation and write behavior must match the established JDBC binding behavior used elsewhere in the driver.

In particular:

- `UTINYINT` must follow the correct `short`-based range semantics, not the incorrect mapping in the current fast implementation
- other unsigned numeric paths must be rechecked against the existing parameter-binding logic and aligned

The redesign should reuse the existing proven numeric conversion rules, not invent new ones.

## Affected files

Primary implementation files:

- `src/main/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatement.java`
- `src/main/java/com/taosdata/jdbc/ws/stmt2/Stmt2ColumnFieldBuffer.java`

Primary cleanup/removal:

- `src/main/java/com/taosdata/jdbc/ws/Stmt2WriteOnceBatchState.java`

Primary tests to update:

- `src/test/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatementTest.java`
- any focused stmt2 fast/serialization tests whose assertions still depend on write-once duplicate detection or old batch-state internals

## Validation plan

### Functional

1. Fast mode setters append directly and build a valid stmt2 bind-exec payload.
2. `executeBatch()` fails when column row counts differ.
3. `executeUpdate()` / `execute()` fail unless all columns contain exactly one row.
4. `tbname` rejects null/empty values.
5. Unsigned-type setters match the driver’s existing binding rules.
6. `clearBatch()` fully resets fast-mode state.
7. close/exception paths still release buffers.

### Performance

Re-run the existing client-only benchmark in `WsStmt2SerializationPerfCompareTest` and compare fast mode against line mode and compatibility column mode. The expectation is that removing setter-time wrappers and bookkeeping reduces the fast-path cost seen in Arthas.

## Rationale

This design intentionally trades JDBC overwrite semantics for a much shorter hot path. The key idea is simple:

- append eagerly
- count cheaply
- validate late

That matches the intended fast-mode contract and aligns the implementation with the user’s stated priority: maximize simplicity and reduce hot-path overhead rather than preserving uncommon PreparedStatement behaviors inside the fast path.
