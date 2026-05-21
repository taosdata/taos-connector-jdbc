# stmt2 write path convergence design

## Problem

The current WebSocket stmt2 write path has accumulated too many runtime variants:

1. `TSWSPreparedStatement`
2. `WSRowPreparedStatement`
3. `WSColumnPreparedStatement`
4. `WSColumnFastPreparedStatement`
5. `WSEWPreparedStatement`
6. user-visible routing knobs (`pbsMode`, `stmt2BindMode`)

This makes routing harder to reason about, keeps dead or near-dead paths alive, and forces users to know internal implementation choices that should instead be decided by server capability.

The intended end state is simpler:

- for **standard JDBC insert writes** on capable servers, use the high-performance columnar `stmt2_bind_exec` path
- for old servers, fall back to the existing legacy implementation
- keep **efficient writing mode** as a separate user-visible mode, but make its internal serializer capability-driven too
- stop exposing line-mode and compatibility-column routing choices as public knobs

## Goals

1. Converge standard JDBC insert writes to only two runtime implementations:
   - capable server: columnar bind-exec
   - old server: legacy `TSWSPreparedStatement`
2. Keep `WSEWPreparedStatement` as the dedicated efficient-writing mode, but make its backend serializer automatically choose columnar bind-exec on capable servers and the current legacy path on old servers.
3. Retire `WSRowPreparedStatement` and `WSColumnPreparedStatement` from the write-path runtime model, with physical deletion allowed after phased validation proves parity.
4. Retire public `pbsMode` and `stmt2BindMode` routing controls from the supported write-path contract, with hard removal allowed in a later cleanup step.
5. Keep routing fully automatic and capability-driven.
6. Preserve current fast-path performance work in `WSColumnFastPreparedStatement`.
7. Deliver the convergence in small, independently testable steps that preserve existing coverage as much as possible.

## Non-goals

1. Add columnar support for query prepared statements.
2. Solve extension API support on capable servers in this phase.
3. Keep line-mode as a supported runtime option.
4. Preserve old compatibility-column mode as a supported runtime option.
5. Introduce a routing facade on every setter hot path.
6. Remove efficient writing mode.

## Implementation constraints

1. Prefer additive, phase-by-phase changes that can be regression-tested independently.
2. Keep code changes narrowly targeted to routing, serializer selection, and directly affected helpers; avoid broad hierarchy reshaping unless it is required to unblock the final convergence.
3. Preserve existing tests and coverage as much as possible. Add focused new tests first; modify old tests only when they assert removed public knobs or deleted runtime classes.

## Approved design

### 1. Runtime routing

`WSConnection.prepareStatement(...)` will be simplified so that standard JDBC insert routing becomes:

1. if the SQL enters **efficient writing mode** (`ASYNC_INSERT` / `asyncWrite=STMT`) and matches the current efficient-writing prerequisites:
   - return `WSEWPreparedStatement`
2. otherwise, if the prepared SQL is an insert and `supportsStmt2BindExec()` is true:
   - return the columnar fast implementation
3. otherwise:
   - return `TSWSPreparedStatement`

The following branches will be removed from standard runtime routing:

- `pbsMode=line`
- `stmt2BindMode=jdbc`
- `WSRowPreparedStatement`
- `WSColumnPreparedStatement`

This makes server capability the only routing decision for standard stmt2 insert writes, while keeping efficient writing as its own explicit user mode.

This is the final routing target. The implementation may reach it in phases; the first steps do not need to physically delete every old class immediately, but each phase should reduce live routing dependence on those old branches.

### 2. Columnar write implementation

The capable-server implementation for phase 1 remains the existing fast-path statement family, centered on `WSColumnFastPreparedStatement`.

However, it should no longer depend on `WSColumnPreparedStatement` staying alive as a separate runtime path. The implementation should be refactored so that:

1. `WSColumnFastPreparedStatement` can stand on its own
2. any still-needed shared logic is moved into a minimal internal helper or base only when necessary
3. deleting `WSColumnPreparedStatement` does not break the standard insert route

This design intentionally keeps the hot path direct:

- no routing facade per setter
- no “first API family decides delegate” wrapper
- no extra forwarding layer in the standard JDBC setter path

The preferred implementation style is to extract only the minimum shared pieces needed for the current phase, not to redesign the whole statement hierarchy in one shot.

### 3. Legacy fallback

`TSWSPreparedStatement` remains the only legacy fallback for old servers in phase 1.

That means the **ordinary write** model becomes:

- **new server** → `WSColumnFastPreparedStatement`
- **old server** → `TSWSPreparedStatement`

No other write-path implementation remains in the standard JDBC insert route.

For **efficient writing mode**, `WSEWPreparedStatement` remains the entrypoint in both cases, but its internal serializer becomes capability-driven:

- **new server** → `WSEWPreparedStatement` with columnar `stmt2_bind_exec` serialization
- **old server** → `WSEWPreparedStatement` with the current legacy EW serialization pipeline

This keeps the user-visible write modes simple:

1. ordinary write
2. efficient write

and makes capability selection internal to each mode where appropriate.

### 4. Extension API scope

Phase 1 explicitly narrows scope to:

1. **standard JDBC insert**
2. **efficient writing mode (`WSEWPreparedStatement`)**

`TaosPrepareStatement` extension APIs such as:

- `setTableName(...)`
- `setTagXxx(...)`
- `columnDataAddBatch()`
- `columnDataExecuteBatch()`

are **not** part of this first convergence step on capable servers.

This is an intentional scope cut to avoid making the first cleanup too complex. The design for extension APIs on the new columnar path is deferred to a later phase.

For phase 1, this means capable-server standard prepared statements do **not** promise `TaosPrepareStatement` extension behavior. If callers need extension APIs, that remains outside the supported scope of this convergence step.

In other words:

- phase 1 converges ordinary write and efficient write
- extension-API convergence is a future design task

### 5. Query behavior

Prepared-statement query behavior remains in the existing legacy path.

This phase does not attempt to:

- columnarize query bind
- change query routing
- merge query behavior into the capable-server fast write implementation

### 6. Public configuration changes

The following public routing knobs should be retired from the supported write-path contract:

- `pbsMode`
- `stmt2BindMode`

The driver should stop advertising these as supported routing controls for stmt2 prepared-statement writes.

Routing becomes purely automatic and based on connected-server capability.

The final state should remove them. To keep rollout small and testable, early phases may temporarily continue parsing or tolerating them while the new routing stops depending on them; hard removal can wait until the final cleanup step.

### 7. Rollout strategy

Implementation should proceed in small, reviewable phases:

1. **Phase 1 – ordinary write routing**
   - narrow standard JDBC insert routing to `WSColumnFastPreparedStatement` on capable servers and `TSWSPreparedStatement` on old servers
   - add focused routing coverage
   - avoid broad edits to existing tests
2. **Phase 2 – efficient writing serializer selection**
   - make `WSEWPreparedStatement` choose columnar vs legacy serialization by capability
   - reuse existing efficient-writing coverage and add only the focused assertions needed for the new branch
3. **Phase 3 – contract cleanup**
   - remove live dependence on old routing knobs and dead branches
   - tighten public behavior and docs once phases 1 and 2 are green
4. **Phase 4 – final physical deletion**
   - delete `WSRowPreparedStatement` and `WSColumnPreparedStatement`
   - update only the tests that directly reference deleted classes or removed public routing controls

Each phase should be independently committable and regression-tested before moving to the next one.

## Affected files

Primary routing and contract changes:

- `src/main/java/com/taosdata/jdbc/ws/WSConnection.java`
- `src/main/java/com/taosdata/jdbc/common/ConnectionParam.java`
- `src/main/java/com/taosdata/jdbc/TSDBDriver.java`

Primary capable-server implementation:

- `src/main/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatement.java`
- any internal stmt2 fast helpers it depends on
- `src/main/java/com/taosdata/jdbc/ws/WSEWPreparedStatement.java`

Primary fallback implementation:

- `src/main/java/com/taosdata/jdbc/ws/TSWSPreparedStatement.java`
- `src/main/java/com/taosdata/jdbc/ws/AbsWSPreparedStatement.java`

Primary deletions (final cleanup step):

- `src/main/java/com/taosdata/jdbc/ws/WSRowPreparedStatement.java`
- `src/main/java/com/taosdata/jdbc/ws/WSColumnPreparedStatement.java`

Primary tests to add/update (prefer additive coverage first):

- `src/test/java/com/taosdata/jdbc/ws/WSConnectionRoutingTest.java`
- `src/test/java/com/taosdata/jdbc/ws/WSConnectionStmt2BindExecTest.java`
- `src/test/java/com/taosdata/jdbc/ws/WSColumnFastPreparedStatementTest.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsPstmtStmt2Test.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsEfficientWritingTest.java`
- `src/test/java/com/taosdata/jdbc/ws/stmt/WsStmtRealPerformanceBenchmarkTest.java`
- Xiaomi/manual benchmark coverage under `src/test/java/com/taosdata/jdbc/ws/manual/`

## Validation plan

### Functional

1. On capable servers, standard JDBC insert prepared statements route to the columnar fast implementation.
2. On old servers, standard JDBC insert prepared statements route to `TSWSPreparedStatement`.
3. On capable servers, `WSEWPreparedStatement` uses columnar bind-exec serialization.
4. On old servers, `WSEWPreparedStatement` keeps using the current legacy EW serialization.
5. Query prepared statements remain on the legacy path.
6. Removing `pbsMode` / `stmt2BindMode` does not leave hidden routing branches behind.
7. Deleting `WSRowPreparedStatement` and `WSColumnPreparedStatement` does not regress standard JDBC insert behavior.

### Compatibility

1. Old-server stmt2 fallback behavior remains correct.
2. Existing standard JDBC insert tests continue to pass on both capable and old-server scenarios.
3. Existing efficient-writing tests continue to pass on both capable and old-server scenarios.
4. Prefer additive focused tests for each phase. Existing suites should remain unchanged unless they assert removed public knobs or deleted concrete types.
5. When an existing test must change, narrow the change to routing expectations or removed public contracts rather than rewriting broad coverage.

### Performance

1. Xiaomi/manual client-only benchmark must not regress against the current fast path.
2. Real benchmark coverage must keep the capable-server columnar route at least non-regressive versus the current branch baseline.
3. No new per-setter routing layer is introduced into the hot path.
4. Efficient-writing mode must not regress on its current throughput path when capability-based serializer selection is added.

## Rationale

This design chooses convergence over keeping every historical routing option alive.

The key decisions are:

1. phase 1 targets ordinary write and efficient write only
2. efficient writing is also part of phase 1, but as its own explicit mode rather than being merged into ordinary routing
3. server capability, not user routing knobs, decides the implementation
4. the fast path must stay direct and hot-path oriented
5. old-server compatibility remains, but only through one legacy implementation for ordinary writes and the existing EW fallback inside `WSEWPreparedStatement`
6. extension-API convergence is valuable, but it is deferred so the first cleanup does not overreach
7. rollout should minimize code churn and preserve existing regression coverage as long as possible

That produces a simpler and easier-to-maintain write-path model without reopening the setter hot-path performance problem that the current fast implementation already solved.
