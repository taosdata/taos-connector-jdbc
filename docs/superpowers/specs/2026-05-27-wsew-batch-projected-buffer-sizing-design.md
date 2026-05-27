# WSEW 按批次写时统计的 buffer sizing 设计

## 1. 设计摘要

这版设计不再走“先 profile 一遍 batch，再建 buffer”的路线，而是改成：

1. 变长列 value buffer 先用一个小的 bootstrap `chunkSize` 起步；
2. 写当前 batch 时，如果当前 chunk 不够，就继续追加 chunk；
3. 写入过程中顺手记录统计信息，不额外重扫数据；
4. 当前 batch 写完后，再根据这批真实写入情况，计算下一批要用的 `wantedSpec`；
5. 下一批开始时，再决定是继续复用、grow，还是在长期 underuse 后 shrink。

本设计的核心目标是：

- **不额外扫描 batch**
- **尽量少引入状态**
- **行为好测试**
- **对大 batch 能迅速调整到更合适的 chunk 规格**

## 2. 为什么要改

当前思路里，WSEW 的 reusable path 只有 `chunkSize` 是动态估出来的，而
`reusableChunkCount` 仍然容易被固定上限卡住。

同时，如果为了 sizing 再额外扫一次当前 batch：

- 会让主路径多一次列数据遍历
- 逻辑侵入感更强
- 也不符合“写的时候就已经知道数据大小”的直觉

因此，这版设计改成**写时顺手收集统计，批末再计算下一批规格**。

## 3. 设计目标

1. 不新增 batch-local profiler，不额外扫描列数据；
2. bootstrap `chunkSize` 足够小，避免一开始就过度分配；
3. 大 batch 出现时，下一批能快速调大到合适规格；
4. shrink 非常慢，避免抖动；
5. payload 格式和编码语义保持不变；
6. 复用 / grow / shrink 的判断可以直接单元测试。

## 4. 非目标

1. 不做机器学习；
2. 不做滚动分位数学习；
3. 不做历史窗口学习；
4. 不做 per-field hard cap；
5. 不在同一批写到一半时切换 `chunkSize`；
6. 不改 fixed-width 字段存储。

## 5. 初始规格

对每个变长列，先定义：

- `chunkBytes`
- `reusableChunkCount`
- `underuseStreak`

其中：

- `chunkBytes`：当前 reusable chunk 的尺寸
- `reusableChunkCount`：当前缓存多少个 reusable chunks
- `underuseStreak`：连续多少批处于“当前规格明显偏大”的状态

新字段第一次进入 WSEW 时，bootstrap 规格定为：

```text
chunkBytes = 8KB
reusableChunkCount = 1
underuseStreak = 0
```

之所以选 `8KB`：

1. 比 `64KB` 明显更保守；
2. 比 `4KB` 更接近 Netty 常见 page 粒度；
3. 在大字段 workload 上，也能在一两批内快速放大。

## 6. 不再新增 batch-local profiler，而是用写时统计 util

引入一个 util，例如：

`ChunkSizingUtil`

它不预扫 rows，而是在当前 batch 写入过程中顺手记录：

- `rowsWritten`
- `observedValueBytes`
- `maxSingleValueBytes`
- `activeChunksUsed`
- `overflowCount`

这些信息都可以在“实际 append value”时拿到，不需要再读第二遍数据。

### 6.1 这个 util 做什么

它只负责两件事：

1. **写时统计**
2. **批末算下一批规格**

它不负责：

- payload 组装
- 中途换规格
- 历史学习

## 7. 当前 batch 的写入规则

当前 batch 进入写入时，直接拿当前规格写：

- 先尝试往当前 chunk 写
- 当前 chunk 不够时，继续追加 chunk
- 当前批内不切换 `chunkSize`

也就是说：

- 当前批只负责把数据写完
- 当前批产生的统计，只影响**下一批**

这样主路径最简单，也最好验证。

## 8. 批末如何算下一批的 `wantedSpec`

当前批写完后，util 用刚才顺手记录的统计量计算下一批规格。

### 8.1 先投影到 `batchSizeByRow`

即使当前只写了半批，也按满批去估：

```text
projectedValueBytes =
    max(
        observedValueBytes,
        ceil(observedValueBytes * batchSizeByRow / rowsWritten)
    )
```

含义：

- 满批：直接用真实值
- 半批：按当前每行平均 bytes 投影到满批

### 8.2 先算理想 `chunkSize`

目标仍然是：

- 理想活跃 chunk 数：`targetActiveChunks = 4`
- 最少不要少于：`minActiveChunks = 2`

先算：

```text
perChunkTarget = projectedValueBytes / targetActiveChunks
chunkCandidate = max(maxSingleValueBytes, perChunkTarget)
dynamicMaxChunkBytes =
    roundUpPow2(
        max(64KB, projectedValueBytes / minActiveChunks)
    )
```

最后：

```text
wantedChunkBytes =
    clamp(
        roundUpPow2(chunkCandidate),
        8KB,
        dynamicMaxChunkBytes
    )
```

### 8.3 再算 `wantedReusableChunkCount`

```text
wantedReusableChunkCount =
    ceil(projectedValueBytes / wantedChunkBytes)
```

## 9. `chunkSize` 计算例子

### 例子 1：`projectedValueBytes = 200KB`

```text
targetActiveChunks = 4
minActiveChunks = 2
perChunkTarget = 200KB / 4 = 50KB
maxSingleValueBytes = 400B
chunkCandidate = max(400B, 50KB) = 50KB
dynamicMaxChunkBytes = roundUpPow2(max(64KB, 200KB / 2))
                     = roundUpPow2(100KB)
                     = 128KB
wantedChunkBytes = clamp(roundUpPow2(50KB), 8KB, 128KB)
                 = 64KB
wantedReusableChunkCount = ceil(200KB / 64KB) = 4
```

### 例子 2：`projectedValueBytes = 800KB`

```text
targetActiveChunks = 4
minActiveChunks = 2
perChunkTarget = 800KB / 4 = 200KB
maxSingleValueBytes = 400B
chunkCandidate = max(400B, 200KB) = 200KB
dynamicMaxChunkBytes = roundUpPow2(max(64KB, 800KB / 2))
                     = roundUpPow2(400KB)
                     = 512KB
wantedChunkBytes = clamp(roundUpPow2(200KB), 8KB, 512KB)
                 = 256KB
wantedReusableChunkCount = ceil(800KB / 256KB) = 4
```

所以：

- 小 batch 仍然可以落在 `64KB`
- 大 batch 会自然放大到 `256KB` 甚至更大
- 不再被固定 `64KB` 卡住

## 10. 下一批开始时的复用 / grow / shrink 判定

下一批开始时，已知：

- 当前规格：`currentSpec`
- 上一批统计出来的下一批目标：`wantedSpec`

### 10.1 先看当前规格是否有资格继续复用

定义：

```text
currentReusableBytes  = current.chunkBytes * current.reusableChunkCount
wantedReusableBytes   = wanted.chunkBytes  * wanted.reusableChunkCount
effectiveActiveChunks = ceil(projectedValueBytes / current.chunkBytes)
```

当前规格只有同时满足下面两个条件，才有资格继续复用：

1. `currentReusableBytes >= wantedReusableBytes`
2. `effectiveActiveChunks <= targetActiveChunks * 2`

当前设计里：

```text
targetActiveChunks = 4
targetActiveChunks * 2 = 8
```

也就是：

- 光“总容量够”还不够
- 如果当前 `chunkSize` 太小，导致这批仍会碎成超过 `8` 个 active chunks
- 那就必须 grow

### 10.2 grow：立即生效

如果当前规格**不满足复用资格**，下一批一开始就重建到 `wantedSpec`，并清零
`underuseStreak`。

### 10.3 same：直接复用

如果 `wantedSpec == currentSpec`：

- 直接复用
- `underuseStreak = 0`

### 10.4 shrink：非常慢

如果当前规格满足复用资格，但 `wantedSpec < currentSpec`：

- 下一批先继续复用
- `underuseStreak += 1`

只有同时满足下面两个条件时，才真正 shrink：

1. `underuseStreak >= 100`
2. `current.chunkBytes * current.reusableChunkCount >= 2 * wanted.chunkBytes * wanted.reusableChunkCount`

也就是说：

- grow 很快
- shrink 很慢
- 并且只有明显 oversized 很久，才会缩

## 11. 为什么这版规则符合当前偏好

### 11.1 不额外扫描

统计都来自实际写入过程，不需要第二次遍历 rows。

### 11.2 没有复杂学习状态

只有一个 `underuseStreak`，它只是 shrink 防抖，不是 learner。

### 11.3 可以快速调大

新字段从 `8KB` 起步；如果第一批很大，第二批就能直接切到更大的 `wantedChunkBytes`。

### 11.4 不会永远卡在很多小 chunk

虽然放宽了复用条件，但还有 `effectiveActiveChunks <= 8` 这条约束，防止系统长期停留在
“容量够、但 chunk 太碎”的状态。

## 12. Dedicated chunk 语义不变

超大单值仍然继续走 dedicated chunk 语义。

这版设计不改：

- payload 格式
- 值编码
- dedicated chunk 基本语义

## 13. 组件改动建议

### 13.1 新增 util

新增一个 util，例如：

- `ChunkSizingUtil`

它负责：

1. 在 append 时顺手统计
2. 在批末计算下一批 `wantedSpec`

### 13.2 `WSEWColumnPreparedStatement`

改成：

1. 用当前规格写当前 batch
2. 写时把统计量交给 util
3. 当前 batch 结束后，算出下一批的 `wantedSpec`
4. 下一批开始时，根据规则做 grow / reuse / shrink

### 13.3 `Stmt2ColumnFieldBuffer`

需要暴露当前规格：

- `chunkBytes`
- `reusableChunkCount`

### 13.4 `ReusableChunkedBuffer`

继续只做 chunk 管理，不负责学习和决策。

## 14. 错误处理和不变量

1. 缺列仍然按当前逻辑抛 `SQLException`
2. 当前批写入过程中，不因为 sizing 决策失败而中途换规格
3. grow / shrink 重建失败时，要释放新建 buffer，并保持异常传播语义不变
4. `rowsWritten == 0` 在真实 WSEW build path 里不是目标分支，不需要单独 fallback

## 15. 测试设计

### 15.1 单元测试

1. **写时统计**
   - append 一组固定值
   - 断言 util 记录的 `observedValueBytes / maxSingleValueBytes / activeChunksUsed` 正确

2. **半批投影**
   - `rowsWritten < batchSizeByRow`
   - 断言 `projectedValueBytes` 按满批投影

3. **chunk size 推导**
   - 输入固定 `projectedValueBytes / maxSingleValueBytes`
   - 断言 `wantedChunkBytes` 正确

4. **chunk count 推导**
   - 断言 `wantedReusableChunkCount = ceil(projectedValueBytes / wantedChunkBytes)`

5. **grow 立即生效**
   - bootstrap `8KB`
   - 第一批写大字段，第二批应立即 grow

6. **容量够但 chunk 太碎时仍 grow**
   - 构造 `currentReusableBytes >= wantedReusableBytes`
   - 但 `effectiveActiveChunks > 8`
   - 断言不能继续复用，必须 grow

7. **shrink 很慢**
   - 连续喂入更小规格 batch
   - 断言前 `99` 批仍复用，第 `100` 批在 oversize 条件满足时 shrink

8. **payload 一致性**
   - 复用路径、grow 路径、shrink 路径产出的 payload 完全一致

### 15.2 回归测试

至少保证下面几组不退：

- `WSEWColumnPreparedStatementTest`
- `ReusableChunkedBufferTest`
- `Stmt2ColumnBindSerializerTest`
- `WsEfficientWritingTest`

## 16. 风险

1. 当前批统计只影响下一批，所以第一批超大 workload 仍可能走很多小 chunk  
   这是为了换取“不额外扫描”和“主路径简单”。

2. 不设 hard cap，极大字段仍可能缓存较多 chunk  
   这是当前设计的明确取舍。

3. `underuseStreak = 100` 会让 shrink 非常慢  
   这是按你的要求故意做的。

## 17. 推荐落地顺序

第一步先实现这版 util 方案：

1. `8KB` bootstrap
2. 写时统计
3. 批末算下一批规格
4. 下一批按规则 grow / reuse / shrink

如果后续 benchmark 证明第一批大 batch 的“冷启动小 chunk 成本”仍太高，再考虑第二阶段：

- 给第一批 bootstrap 加一个更聪明的初始猜测
- 但仍然不引入额外扫描
