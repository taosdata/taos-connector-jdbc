# tbname string hot-path design

## Problem

`WSColumnFastPreparedStatement` now uses the simplified direct-append fast path, but `tbname` is still heavier than it needs to be.

Today `Stmt2ColumnFieldBuffer.appendTbNameValue(String)`:

1. writes the string into the value buffer
2. encodes the same string again with `getBytes(UTF_8)`
3. compares both string and byte caches to decide whether to increment `tableCount`

That means the common `setString(tbname, ...)` path still pays for:

- double UTF-8 work
- extra byte-array allocation
- extra byte comparison/copy bookkeeping

This is exactly the wrong tradeoff for the current fast-mode goal. The priority is to make `setString(tbname, ...)` as hot as possible, even if `setBytes(tbname, ...)` becomes a compatibility-only path with worse performance.

## Goals

1. Make `setString(tbname, ...)` the fastest supported `tbname` path in fast mode.
2. Remove byte-cache bookkeeping from the hot `tbname` string path.
3. Keep `setBytes(tbname, ...)` functionally correct, but allow it to be slower.
4. Keep mixed `setString(tbname, ...)` and `setBytes(tbname, ...)` behavior correct.
5. Reject malformed UTF-8 when `setBytes(tbname, ...)` is used.

## Non-goals

1. Optimize `setBytes(tbname, ...)` for throughput.
2. Preserve raw byte-level `tbname` semantics for malformed UTF-8 input.
3. Change non-`tbname` string/bytes behavior.
4. Change stmt2 payload format or table-count meaning.

## Approved design

### 1. Normalize `tbname` onto a single String-based path

Fast mode will treat `tbname` as having one canonical in-memory form: `String`.

- `setString(tbname, ...)` uses that form directly.
- `setBytes(tbname, ...)` must first decode bytes to `String` with strict UTF-8 rules.
- after decode, `setBytes(tbname, ...)` reuses the exact same append path as `setString(tbname, ...)`.

This means fast mode no longer needs separate byte-cache state to track previous `tbname` values.

### 2. Hot path behavior for `setString(tbname, ...)`

`appendTbName(String name)` becomes the dedicated hot path.

Its behavior is:

1. reject null or empty input
2. append the UTF-8 form to `valueBuffer` with a single `writeString(name)` call
3. write the returned UTF-8 byte length into `lengthBuffer`
4. increment `rowCount`
5. compare `name` against `lastTableName`
6. if different, increment `tableCount`
7. store the new `lastTableName`

The string hot path must not:

- call `name.getBytes(StandardCharsets.UTF_8)`
- compare against cached `byte[]`
- copy previous UTF-8 bytes

### 3. Compatibility path for `setBytes(tbname, ...)`

When the target field is `tbname`, `setBytes(...)` becomes:

1. reject null or empty input
2. decode bytes using UTF-8 with malformed/unmappable input configured as `REPORT`
3. if decode fails, throw `SQLException`
4. if decode succeeds, call the same String-based `tbname` append path used by `setString(...)`

This keeps behavior correct while explicitly accepting the extra decode cost on the bytes path.

### 4. State simplification

`Stmt2ColumnFieldBuffer` will keep:

- `int tableCount`
- `String lastTableName`

and remove `tbname`-specific byte-cache state:

- `byte[] lastTableNameBytes`
- `int lastTableNameLength`
- `sameBytes(...)`
- `copyPrefix(...)`

The buffer continues to own `rowCount`, `nullBuffer`, `lengthBuffer`, and `valueBuffer` as before.

### 5. Mixed usage behavior

Mixed `setString(tbname, ...)` and `setBytes(tbname, ...)` remains correct because both paths are normalized into the same String-based append logic before `tableCount` is updated.

There is no execute-time recount fallback in this design.

The accepted tradeoff is simple:

- `setString(tbname, ...)` is optimized
- `setBytes(tbname, ...)` is slower because it decodes first

### 6. Error handling

The fast-mode `tbname` rules become:

- `setString(tbname, null)` -> fail
- `setString(tbname, "")` -> fail
- `setBytes(tbname, null)` -> fail
- `setBytes(tbname, new byte[0])` -> fail
- `setBytes(tbname, invalid UTF-8)` -> fail

These failures happen before any row is appended to the column buffer.

## Affected files

Primary implementation files:

- `src/main/java/com/taosdata/jdbc/ws/stmt2/Stmt2ColumnFieldBuffer.java`
- `src/main/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatement.java`

Primary tests:

- `src/test/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatementTest.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsStmt2SerializationPerfCompareTest.java`

## Validation plan

### Functional

1. `setString(tbname, ...)` still rejects null/empty input.
2. `setBytes(tbname, validUtf8)` behaves the same as the equivalent `setString(tbname, decodedString)`.
3. `setBytes(tbname, invalidUtf8)` fails immediately.
4. repeated same-table and table-switch sequences still compute `tableCount` correctly.
5. mixed `setString(tbname, ...)` and `setBytes(tbname, ...)` sequences still compute `tableCount` correctly.

### Performance

Re-run the existing client-only benchmark with the workload shape the user cares about:

- `rows=10000`
- `rounds=100`

The expectation is not necessarily to beat `WSRowPreparedStatement` immediately, but to reduce avoidable `tbname` overhead on the fast path by removing the second UTF-8 encode and byte-cache bookkeeping.

## Rationale

The design deliberately chooses one optimized representation for `tbname` in fast mode: `String`.

That keeps the common path short and predictable:

- one validation
- one encode
- one equality check

instead of trying to make both String and bytes equally fast and reintroducing the kind of cross-path state that already hurt fast-mode performance. The bytes setter remains supported, but only as a compatibility path.
