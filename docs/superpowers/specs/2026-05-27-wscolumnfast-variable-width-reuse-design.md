# WSColumnFastPreparedStatement 变长列 chunk reuse 设计

## 1. 设计摘要

这版设计的目标不是把 `WSEWColumnPreparedStatement` 的实现整体搬到
`WSColumnFastPreparedStatement`，而是：

1. **最大化复用**已经在 WSEW 中验证过的 chunk reuse 基础设施；
2. **最小化侵入** `WSColumnFastPreparedStatement` 的现有同步写入路径；
3. 在 batch 大小可能忽大忽小的前提下，优先保证**写入性能稳定**，允许适度内存浪费；
4. 让变长列复用策略偏向 **大 chunk、少 chunk、极慢 shrink**。

核心结论：

- `ReusableChunkedBuffer`
- `Stmt2ColumnFieldBuffer.forReusableValueBuffer(...)`
- `WSEWChunkSizingUtil.BufferSpec`
- `WSEWChunkSizingUtil.FieldBatchStats`

这些基础能力可以直接复用。

但 `WSColumnFastPreparedStatement` **不适合**直接复用 WSEW 那套“按
`batchSizeByRow` 投影下一批规格”的策略，因为它面对的 batch 天然更不稳定。

因此，这里采用：

- **公共 buffer / state / helper 复用**
- **WSColumnFast 自己使用更保守的、反应式 sizing 策略**

## 2. 当前代码现状

### 2.1 WSColumnFast 当前对变长列的处理

`WSColumnFastPreparedStatement` 当前的变长列生命周期在：

- `WSColumnFastPreparedStatement.java:735-759`

现状是：

1. 初始化时通过 `allocateColumnBuffers()` 创建 `Stmt2ColumnFieldBuffer`
2. 写入时直接走 `stageVar()` / `stageString()` append 到列 buffer
3. `executeBatch()` 完成后进入 `resetFastState()`
4. `resetFastState()` 对变长列不是 `reset()`，而是：
   - `snapshotUsage()`
   - `release()`
   - `new Stmt2ColumnFieldBuffer(fieldMetas[i], hints)`

也就是说，当前只有“**按上一批用量给 hints**”，没有真正的 **chunk cache 复用**。

### 2.2 WSEW 已经具备的可复用能力

WSEW 当前已经验证了以下能力：

1. 变长列底层可切到 `ReusableChunkedBuffer`
2. `Stmt2ColumnFieldBuffer` 已经支持暴露：
   - `currentReusableSpec()`
   - `activeReusableChunkCount()`
3. `WSEWChunkSizingUtil` 已经提供：
   - `BufferSpec`
   - `FieldBatchStats`
   - bootstrap spec
   - grow / reuse / shrink 判断基础
4. `WSEWColumnPreparedStatement` 已经跑通：
   - 当前批按当前 spec 写
   - 批末根据 stats 决定下一批 spec
   - 下批再生效

因此，WSColumnFast 缺的不是基础设施，而是：

- **如何以低侵入方式接入这些基础设施**
- **如何把 sizing 策略改成适合不稳定 batch 的版本**

## 3. 设计目标

1. 复用 WSEW 已有 chunk reuse 基础设施，而不是重写一套新 buffer；
2. 不改 `Stmt2ColumnBindSerializer` 和现有 stmt2 binary payload 格式；
3. 不改 fixed-width 列路径；
4. 不让一两次小 batch 导致 spec 立刻回缩；
5. 当 chunk 个数已经很多时，允许 grow，而且优先变成更大的 chunk；
6. shrink 必须非常慢；
7. 代码改动尽量集中在：
   - 一个新的公共 helper
   - `WSColumnFastPreparedStatement`

## 4. 非目标

1. 不把 `WSColumnFastPreparedStatement` 改成 WSEW 那种后台线程 / queue 模型；
2. 不引入 batch 大小预测；
3. 不根据“猜测的下一批规模”提前放大；
4. 不在当前批写到一半时修改 `chunkBytes`；
5. 不把 shrink 做成积极回收内存的策略；
6. 不做多 bucket 方案。

## 5. 备选方案与取舍

### 方案 A：直接把 WSEW 代码复制到 WSColumnFast

优点：

- 开发快

缺点：

- 重复代码多
- WSEW 的 queue/后台线程语义并不适用于 WSColumnFast
- 后续维护两套 grow/reuse/shrink 状态机风险高

结论：**不选**

### 方案 B：抽出公共 helper，WSColumnFast 只做薄接入

优点：

- 最大化复用 WSEW 已有优化
- 侵入面最小
- 后续两条路径共享同一套 buffer 和 state 语义

缺点：

- 需要先把 WSEW 中部分逻辑拆成通用 helper

结论：**推荐方案**

### 方案 C：WSColumnFast 先只做 grow-only，不做任何 shrink

优点：

- 抖动最小
- 实现最简单

缺点：

- 一次大 batch 可能把 statement 生命周期内的内存水位长期抬高
- 不能满足“允许极慢 shrink”的需求

结论：可以作为降级 fallback，但不是这次设计的首选。

## 6. 推荐设计：抽公共 helper，WSColumnFast 薄接入

### 6.1 代码复用边界

建议把当前 WSEW 中已经被验证、且不依赖异步队列的部分抽成一个公共 helper，
放在 `stmt2` 包下。

这个 helper 只负责：

1. 按 `BufferSpec` 创建变长列 reusable buffer；
2. 预热 reusable chunk cache；
3. 比较当前 buffer spec 与目标 spec；
4. 维护 `nextSpecs[]` 与 `underuseStreaks[]`；
5. 根据本批 stats 计算下一批 spec。

这个 helper **不负责**：

- queue
- 后台线程
- `EWBackendThreadInfo`
- row list -> column buffer 的组装

这样：

- WSEW 继续保留它自己的 queue / task 部分
- WSColumnFast 只复用公共 helper 和已有 buffer 基础设施

### 6.2 WSColumnFast 自身只保留薄接入点

`WSColumnFastPreparedStatement` 只在三个位置接入：

1. **分配 buffer**
   - 变长列不再走普通 `new Stmt2ColumnFieldBuffer(...)`
   - 而是从 `currentSpec / nextSpec` 创建 `forReusableValueBuffer(...)`

2. **写入时收集 stats**
   - `stageVar()`
   - `stageString()`
   - `tbname` 写入路径
   - `stageNull()`

   在真实 append 时顺手更新 `FieldBatchStats`

3. **批成功后更新下一批 spec**
   - `executeBatch()` 成功发出并拿到结果后
   - 再用本批 stats 更新 `nextSpecs[]`

除此之外：

- serializer 不变
- fixed-width 路径不变
- 当前 stmt2 prepare / execute protocol 不变

## 7. WSColumnFast 的状态模型

与 WSEW 不同，这里的状态全部挂在 statement 自身，不引入新的外部共享对象。

每个变长列维护三类状态：

1. `current/next BufferSpec`
2. `underuseStreak`
3. 本批 `FieldBatchStats`

推荐语义：

- `nextSpecs[i]`
  - 代表下一批应该使用的规格
- `underuseStreaks[i]`
  - 记录连续多少批明显低于当前容量
- `batchStats[i]`
  - 只统计本批真实写入结果
  - 每次 `executeBatch()` 后重置

fixed-width 列不持有这组状态。

## 8. sizing 策略：反应式，不预测

### 8.1 基本原则

WSColumnFast 不像 WSEW 那样有稳定 `batchSizeByRow`，因此这里**不做下一批投影**。

也就是说：

- 只看本批真实 observed bytes
- 不乘“未来 batch 可能更大”的放大系数
- 当前批只负责写完
- 下一批才应用新 spec

### 8.2 bootstrap

每个变长列第一次进入 reusable 模式时：

```text
chunkBytes = 8KB
reusableChunkCount = 1
underuseStreak = 0
```

### 8.3 grow 规则

当前批结束后，只要满足以下任一条件，下一批允许 grow：

1. `overflowCount > 0`
2. `activeChunksUsed > 8`
3. `observedValueBytes > currentReusableBytes`

grow 时遵循：

- **优先放大 chunkBytes**
- 目标是回到“大 chunk、少 chunk”
- 不是优先堆更多小 chunk

推荐做法：

1. 先根据本批 observed bytes 和 `maxSingleValueBytes` 算出一个候选 `wantedChunkBytes`
2. 让它提升到不小于当前 `chunkBytes` 的下一个 2 的幂
3. 再计算需要多少 `reusableChunkCount`

也就是说，grow 的顺序是：

```text
先放大 chunkBytes
再补足 reusableChunkCount
```

而不是：

```text
先继续加很多小 chunk
```

### 8.4 reuse 规则

reuse 采用**容量优先、结构次之**的保守策略：

- 只要当前 spec 没有 overflow
- 且当前容量还能覆盖本批真实数据
- 就优先继续复用当前 spec

这里不把 `activeChunksUsed` 当作强制切换 spec 的轻微门槛。

只有当它真的已经很多（例如超过 8）时，才认为需要 grow。

换句话说：

- 4~6 个活跃 chunk 不是问题本身
- 频繁切 spec 才是更大的问题

### 8.5 shrink 规则

shrink 设得非常慢。

只有当以下条件同时满足时，才允许 shrink：

1. 连续 `100` 批
2. 每一批 `observedValueBytes < currentReusableBytes / 2`

一旦满足，也**不是一次性缩大步**，而是：

1. **优先只减少一个 reusable chunk**
2. 只有 `reusableChunkCount == 1` 之后
3. 才考虑把 `chunkBytes` 降一级

也就是说 shrink 是：

```text
慢
保守
一次只缩一小步
```

这样可以保证：

- 一次偶发小 batch 不会导致回缩
- 几次连续小 batch 也不会导致回缩
- 只有长期都显著偏小，才逐步回收容量

## 9. 为什么这版策略更适合 WSColumnFast

这版设计更适合 WSColumnFast 的原因是：

1. WSColumnFast 的 batch 规模可能剧烈波动；
2. 直接照搬 WSEW 的“按满批投影”会更容易误判；
3. 用户偏好是：
   - 宁可多占一点内存
   - 也不要引入写入性能抖动

因此，这里故意做成：

- grow 可以快一点
- shrink 必须非常慢
- reuse 要尽量 sticky

这和 WSEW 的设计目标不同，但底层 buffer / spec / stats 仍然可以复用。

## 10. 失败处理与边界行为

自适应逻辑只影响**下一批** buffer 规格，不影响当前批已经构造好的 payload。

因此，要求：

1. 当前批成功写出后，才能更新下一批 spec；
2. 当前批 payload 一旦完成，不因为 sizing 更新失败而回滚；
3. sizing 更新出错时，保守做法是：
   - 继续保留当前 spec
   - 不让本次成功写入受影响

这一点的优先级高于“本批就一定要把 spec 学对”。

## 11. 测试重点

至少覆盖四类测试：

### 11.1 复用行为

验证同一个 `PreparedStatement` 连续多次 `executeBatch()` 后：

- 变长列 buffer 被复用
- 不是每批 `release + new`

### 11.2 grow 行为

验证当本批：

- overflow
- 或 active chunk 个数很多

下一批确实 grow，而且 grow 结果偏向：

- 更大的 `chunkBytes`
- 而不是更多的小 chunk

### 11.3 shrink 行为

验证：

- 连续不到 100 批，不 shrink
- 连续 100 批都低于当前容量一半，才 shrink
- shrink 一次只减少一个 reusable chunk

### 11.4 抖动抑制

验证 batch 大小忽大忽小时：

- 一两次小 batch 不会把前面为大 batch 拉高的 spec 立刻缩回去
- spec 不会在相邻批次里频繁来回切换

## 12. 实现优先级建议

建议分两步推进：

### 第一步：只做公共 helper 抽取 + chunk reuse 接入

目标：

- 先把 `release + new` 变成真实跨批复用
- 不急着做完整自适应

### 第二步：再接入本设计的极慢 shrink / 大 chunk 优先 grow

目标：

- 在已有复用基础上，把 batch 抖动下的性能稳定性调优到位

这样做的好处是：

- 复用收益可以先拿到
- 风险最小
- 代码复用边界最清晰

## 13. 最终结论

`WSColumnFastPreparedStatement` 的变长列**非常适合**参考 WSEW 的 chunk reuse
方式，但不应该直接复制 WSEW 的整套实现。

最合理的路线是：

1. 把 WSEW 中与 queue 无关的 reusable buffer / spec / state 逻辑抽成公共 helper；
2. 让 `WSColumnFastPreparedStatement` 以极薄的方式接入；
3. sizing 策略改成适合不稳定 batch 的版本：
   - **大 chunk 优先**
   - **reuse 更 sticky**
   - **shrink 极慢**

这条路线同时满足：

- **代码复用最大化**
- **侵入性最小**
- **写入性能抖动最小**
