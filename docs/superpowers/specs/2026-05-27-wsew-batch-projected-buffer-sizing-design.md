# WSEW batch-projected buffer sizing design

## Summary

This design replaces the current fixed `EW_MAX_REUSABLE_CHUNKS` heuristic in the
WSEW columnar serialization path with a deterministic, batch-local sizing
strategy.

For each variable-width field, the serializer will:

1. profile the rows currently dequeued for serialization,
2. project the observed value bytes to the configured `batchSizeByRow`,
3. derive a `BufferSpec = { chunkBytes, reusableChunkCount }`,
4. reuse the existing field buffer only when its spec exactly matches the new
   one, otherwise rebuild that field buffer for the next batch.

The design intentionally avoids long-lived learning state, per-field hard caps,
and incremental in-place resizing logic. The goal is to make the behavior easy
to reason about, easy to test, and safe to evolve without changing payload
correctness.

## Problem

The current WSEW reusable path computes:

- `standardChunkBytes` from `field.bytes` and `batchSizeHint`
- `EW_MAX_REUSABLE_CHUNKS` from a fixed constant (`4`)

That works when the projected value area for a field fits within the cached
chunk budget, but it under-covers larger batches. In the `extension=400B`
example:

- `500` rows are close to `4 * 64KB`, so reuse helps
- `2000` rows need roughly `800KB`, which is much larger than `4 * 64KB`

As a result, the current reusable path may still allocate and release a large
fraction of the chunks for bigger batches, reducing the benefit of cross-batch
reuse.

## Goals

1. Size variable-width reusable buffers according to the configured
   `batchSizeByRow`, not just the rows currently observed in a partial batch.
2. Keep the sizing logic deterministic and easy to test.
3. Preserve the current payload format and correctness guarantees.
4. Avoid hidden learning state that could make behavior hard to explain or debug.
5. Let stable workloads converge quickly to a stable reusable buffer shape.

## Non-goals

1. No adaptive machine-learning model.
2. No percentile learner or rolling history in the initial design.
3. No per-field hard memory cap in the initial design.
4. No change to dedicated-chunk semantics for very large single values.
5. No change to fixed-width field storage.

## Existing constraints

The design works with the current WSEW flow:

- rows are already materialized into a `List<Map<Integer, Column>>` before
  column buffers are built,
- `triggerSerializeProgressive(...)` only submits work when the write queue is
  non-empty,
- `ColumnarWSEWSerializationTask.compute()` dequeues exactly `batchSize` rows
  before building buffers,
- therefore the build path never needs a zero-row fallback.

This means we can safely profile the rows for the current serialization task
before allocating or rebuilding the variable-width field buffers.

## Proposed design

### 1. Introduce a deterministic `BufferSpec`

For every variable-width field, define:

- `chunkBytes`
- `reusableChunkCount`

This pair is the full reusable-buffer shape for that field. If two batches
derive the same spec, the reusable buffer is safe to keep. If the derived spec
changes, the buffer should be rebuilt instead of mutated in place.

### 2. Add a batch-local profiling pass

Before creating or reusing the field buffers for a WSEW batch, scan the dequeued
rows and compute, for each variable-width field:

- `observedRows`
- `observedValueBytes`
- `maxSingleValueBytes`
- `nonNullRows`

`observedValueBytes` must be the encoded value-byte count that the serializer
would append for that field, not the Java object count.

This pass is local to the current batch only. No historical profile state is
kept.

### 3. Project to the configured full-batch target

The sizing target must always be the configured `batchSizeByRow`, even when the
currently dequeued batch is smaller.

For each variable-width field:

```text
projectedValueBytes =
    max(observedValueBytes,
        ceil(observedValueBytes * batchSizeByRow / observedRows))
```

This means:

- full batches use their actual observed size,
- partial batches are scaled up to the equivalent full-batch estimate,
- the sizing decision is not biased downward by a short progressive batch.

### 4. Derive `chunkBytes` from the projected batch

The implementation will keep bucketed chunk sizes, but the bucket choice is
dynamic per batch.

Use:

```text
idealChunkBytes =
    clamp(
        roundUpPow2(
            max(maxSingleValueBytes,
                projectedValueBytes / targetActiveChunks)
        ),
        8KB,
        64KB
    )
```

with:

- `targetActiveChunks = 4`
- `roundUpPow2` producing the next power-of-two bucket
- the existing `8KB .. 64KB` bounds retained

The bucket set is fixed because that makes pooled allocation predictable and
test-friendly, but the selected bucket is still computed from the current batch
profile.

### 5. Derive `reusableChunkCount` from the projected batch

After `idealChunkBytes` is chosen:

```text
requiredReusableChunks =
    ceil(projectedValueBytes / idealChunkBytes)
```

This removes the fixed `EW_MAX_REUSABLE_CHUNKS = 4` limit from the sizing
decision. The reusable chunk count now scales with the projected full-batch
value size for that field.

### 6. Reuse vs rebuild rule

For each variable-width field:

- if `currentSpec == wantedSpec`, reuse the existing field buffer,
- if `currentSpec != wantedSpec`, release the old field buffer and create a new
  one with `wantedSpec`.

This is intentionally stricter than “resize in place”:

- behavior is deterministic,
- tests can assert exact reuse/rebuild outcomes,
- old oversized caches do not survive indefinitely after the workload changes,
- there is no hidden migration logic to debug.

### 7. Dedicated-chunk behavior stays unchanged

Single values that are large enough to trigger the dedicated-chunk path will
continue to do so. The new sizing logic only changes:

- reusable chunk size selection,
- reusable chunk count selection,
- the decision to reuse vs rebuild.

The payload format and value encoding remain unchanged.

## Why bucketed chunk sizes remain the right choice

The design keeps bucketed chunk sizes because exact byte-for-byte chunk sizing
would work against the goals:

1. tiny per-batch changes would cause frequent spec churn,
2. pooled allocation reuse would be worse,
3. expected behavior would be harder to test because every input delta could
   produce a new exact size.

Power-of-two buckets within the existing min/max range keep the behavior stable,
while still allowing the selected bucket to change dynamically with the batch
profile.

## Component changes

### `WSEWColumnPreparedStatement`

Add a helper that profiles the current dequeued rows for variable-width fields
and computes `BufferSpec[]` for the current task.

Change the reusable build path so that:

1. it profiles the current rows,
2. derives `wantedSpec[]`,
3. reuses or rebuilds each variable-width field buffer by exact-spec match,
4. fills the chosen buffers with the actual batch rows.

### `Stmt2ColumnFieldBuffer`

Expose enough metadata to compare the current reusable configuration against
`wantedSpec`, for example:

- current `chunkBytes`
- current `reusableChunkCount`

No payload-layout change is required.

### `ReusableChunkedBuffer`

Construct it from the new per-field spec:

- `chunkBytes`
- `reusableChunkCount`

No online resizing is needed. Reconfiguration happens by replacing the field
buffer when the spec changes.

## Error handling and invariants

1. If a row is missing a bound column, keep throwing `SQLException` as today.
2. If profiling detects no variable-width data for a field, the field still gets
   a deterministic spec derived from the projected full-batch formula.
3. `observedRows == 0` is not an implementation path and should not get a
   dedicated fallback branch in WSEW.
4. If a rebuild is required and later filling fails, release the newly built
   buffers and propagate the failure exactly as today.

## Testing plan

### Unit tests

1. **Half-batch projection**
   - input: `observedRows < batchSizeByRow`
   - assert that the derived `wantedSpec` matches the projected full-batch size
2. **Stable workload reuse**
   - same batch shape repeated
   - assert first batch builds, later batches reuse
3. **Spec-switch rebuild**
   - small-batch shape then large-batch shape
   - assert the buffer is rebuilt when `wantedSpec` changes
4. **Payload equality**
   - same rows through rebuilt and reused paths
   - assert identical serialized payload bytes
5. **Large single-value floor**
   - assert `maxSingleValueBytes` can force a larger bucket when needed

### Regression tests

Keep the current WSEW / stmt2 serializer regression set green, especially:

- `WSEWColumnPreparedStatementTest`
- `ReusableChunkedBufferTest`
- `Stmt2ColumnBindSerializerTest`
- `WsEfficientWritingTest`

## Risks

1. If the current batch is not representative, the projected full-batch size may
   still be imperfect. This is acceptable because the design optimizes for
   deterministic behavior rather than hidden learning state.
2. Removing the fixed reusable-chunk cap can increase retained memory for very
   large fields. This is an intentional trade-off in the first design version
   because the user explicitly chose not to keep a hard cap.
3. Exact-spec rebuilds may rebuild more often than a softer hysteresis design,
   but they are much easier to test and reason about.

## Rollout recommendation

Implement this design first without any learner or historical state.

If later measurements show that exact-spec rebuilds are still too expensive for
mixed workloads, the next incremental step should be a minimal-state
`lastProjectedSpec` reuse policy. That can be evaluated separately without
changing the core deterministic profiler introduced here.
