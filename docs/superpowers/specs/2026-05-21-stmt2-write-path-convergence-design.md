# stmt2 write path convergence design

## Problem

The current WebSocket stmt2 write path has accumulated too many runtime variants:

1. `TSWSPreparedStatement`
2. `WSRowPreparedStatement`
3. `WSColumnPreparedStatement`
4. `WSColumnFastPreparedStatement`
5. user-visible routing knobs (`pbsMode`, `stmt2BindMode`)

This makes routing harder to reason about, keeps dead or near-dead paths alive, and forces users to know internal implementation choices that should instead be decided by server capability.

The intended end state is simpler:

- for **standard JDBC insert writes** on capable servers, use the high-performance columnar `stmt2_bind_exec` path
- for old servers, fall back to the existing legacy implementation
- stop exposing line-mode and compatibility-column routing choices as public knobs

## Goals

1. Converge standard JDBC insert writes to only two runtime implementations:
   - capable server: columnar bind-exec
   - old server: legacy `TSWSPreparedStatement`
2. Remove `WSRowPreparedStatement` and `WSColumnPreparedStatement` from the write-path runtime model.
3. Remove public `pbsMode` and `stmt2BindMode` routing controls.
4. Keep routing fully automatic and capability-driven.
5. Preserve current fast-path performance work in `WSColumnFastPreparedStatement`.

## Non-goals

1. Add columnar support for query prepared statements.
2. Solve extension API support on capable servers in this phase.
3. Keep line-mode as a supported runtime option.
4. Preserve old compatibility-column mode as a supported runtime option.
5. Introduce a routing facade on every setter hot path.

## Approved design

### 1. Runtime routing

`WSConnection.prepareStatement(...)` will be simplified so that standard JDBC insert routing becomes:

1. if the prepared SQL is an insert and `supportsStmt2BindExec()` is true:
   - return the columnar fast implementation
2. otherwise:
   - return `TSWSPreparedStatement`

The following branches will be removed from standard runtime routing:

- `pbsMode=line`
- `stmt2BindMode=jdbc`
- `WSRowPreparedStatement`
- `WSColumnPreparedStatement`

This makes server capability the only routing decision for standard stmt2 insert writes.

### 2. Columnar write implementation

The capable-server implementation for phase 1 remains the existing fast-path statement family, centered on `WSColumnFastPreparedStatement`.

However, it should no longer depend on `WSColumnPreparedStatement` staying alive as a separate runtime path. The implementation should be refactored so that:

1. `WSColumnFastPreparedStatement` can stand on its own
2. any still-needed shared logic is moved into a minimal internal helper or base
3. deleting `WSColumnPreparedStatement` does not break the standard insert route

This design intentionally keeps the hot path direct:

- no routing facade per setter
- no “first API family decides delegate” wrapper
- no extra forwarding layer in the standard JDBC setter path

### 3. Legacy fallback

`TSWSPreparedStatement` remains the only legacy fallback for old servers in phase 1.

That means the write-path model becomes:

- **new server** → `WSColumnFastPreparedStatement`
- **old server** → `TSWSPreparedStatement`

No other write-path implementation remains in the standard JDBC insert route.

### 4. Extension API scope

Phase 1 explicitly narrows scope to **standard JDBC insert** only.

`TaosPrepareStatement` extension APIs such as:

- `setTableName(...)`
- `setTagXxx(...)`
- `columnDataAddBatch()`
- `columnDataExecuteBatch()`

are **not** part of this first convergence step on capable servers.

This is an intentional scope cut to avoid making the first cleanup too complex. The design for extension APIs on the new columnar path is deferred to a later phase.

For phase 1, this means capable-server standard prepared statements do **not** promise `TaosPrepareStatement` extension behavior. If callers need extension APIs, that remains outside the supported scope of this convergence step.

In other words:

- phase 1 converges the standard JDBC insert path
- extension-API convergence is a future design task

### 5. Query behavior

Prepared-statement query behavior remains in the existing legacy path.

This phase does not attempt to:

- columnarize query bind
- change query routing
- merge query behavior into the capable-server fast write implementation

### 6. Public configuration changes

The following public routing knobs should be removed from the phase-1 write-path contract:

- `pbsMode`
- `stmt2BindMode`

The driver should stop advertising these as supported routing controls for stmt2 prepared-statement writes.

Routing becomes purely automatic and based on connected-server capability.

To avoid silent behavior changes, phase 1 should fail explicitly when these removed routing properties are supplied, instead of silently honoring or ignoring them.

## Affected files

Primary routing and contract changes:

- `src/main/java/com/taosdata/jdbc/ws/WSConnection.java`
- `src/main/java/com/taosdata/jdbc/common/ConnectionParam.java`
- `src/main/java/com/taosdata/jdbc/TSDBDriver.java`

Primary capable-server implementation:

- `src/main/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatement.java`
- any internal stmt2 fast helpers it depends on

Primary fallback implementation:

- `src/main/java/com/taosdata/jdbc/ws/TSWSPreparedStatement.java`
- `src/main/java/com/taosdata/jdbc/ws/AbsWSPreparedStatement.java`

Primary deletions:

- `src/main/java/com/taosdata/jdbc/ws/WSRowPreparedStatement.java`
- `src/main/java/com/taosdata/jdbc/ws/WSColumnPreparedStatement.java`

Primary tests to update:

- `src/test/java/com/taosdata/jdbc/ws/WSConnectionRoutingTest.java`
- `src/test/java/com/taosdata/jdbc/ws/WSConnectionStmt2BindExecTest.java`
- `src/test/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatementTest.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsPstmtStmt2Test.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsStmtRealPerformanceBenchmarkTest.java`
- Xiaomi/manual benchmark coverage under `src/test/java/com/taosdata/jdbc/ws/manual/`

## Validation plan

### Functional

1. On capable servers, standard JDBC insert prepared statements route to the columnar fast implementation.
2. On old servers, standard JDBC insert prepared statements route to `TSWSPreparedStatement`.
3. Query prepared statements remain on the legacy path.
4. Removing `pbsMode` / `stmt2BindMode` does not leave hidden routing branches behind.
5. Deleting `WSRowPreparedStatement` and `WSColumnPreparedStatement` does not regress standard JDBC insert behavior.

### Compatibility

1. Old-server stmt2 fallback behavior remains correct.
2. Existing standard JDBC insert tests continue to pass on both capable and old-server scenarios.
3. Any tests that relied on unwrapping specific deleted classes are updated to assert behavior rather than old concrete types.

### Performance

1. Xiaomi/manual client-only benchmark must not regress against the current fast path.
2. Real benchmark coverage must keep the capable-server columnar route at least non-regressive versus the current branch baseline.
3. No new per-setter routing layer is introduced into the hot path.

## Rationale

This design chooses convergence over keeping every historical routing option alive.

The key decisions are:

1. standard JDBC insert writes are the only phase-1 target
2. server capability, not user routing knobs, decides the implementation
3. the fast path must stay direct and hot-path oriented
4. old-server compatibility remains, but only through one legacy implementation
5. extension-API convergence is valuable, but it is deferred so the first cleanup does not overreach

That produces a simpler and easier-to-maintain write-path model without reopening the setter hot-path performance problem that the current fast implementation already solved.
