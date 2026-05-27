# WSEW 按满批投影的 buffer sizing 设计

## 1. 设计摘要

这个设计要解决的是：WSEW 变长列的 `ReusableChunkedBuffer` 现在只会动态算
`chunkSize`，但 `reusableChunkCount` 还是固定常量，导致大 batch 下复用覆盖不全。

这次设计改成一套**确定性的、按当前 batch 本地计算的 sizing 规则**：

1. 先 profile 当前要序列化的 rows；
2. 即使当前 rows 不满，也按 `batchSizeByRow` 投影到“满批”规模；
3. 对每个变长列算出一个 `BufferSpec = { chunkBytes, reusableChunkCount }`；
4. `currentSpec == wantedSpec` 才复用，否则整列重建；
5. 不引入历史学习状态，不引入 per-field hard cap，不改 payload 格式。

核心目标是：**行为可预测、好测试、不因为“学习逻辑”引入隐蔽 bug。**

## 2. 当前问题

现在 WSEW 可复用路径的关键参数是：

- `standardChunkBytes`：由 `field.bytes` 和 `batchSizeHint` 推出来
- `EW_MAX_REUSABLE_CHUNKS`：固定常量 `4`

这个组合在小一些的 batch 上还能工作，但在大一些的 batch 上会出现：

- chunk size 已经顶到 `64KB`
- 但 cached reusable chunk 还是只有 `4`
- 所以后面很多 chunk 还是每批现申请、现释放

例如 `extension=400B`：

- `500` 行大约是 `200KB`，接近 `4 * 64KB`
- `2000` 行大约是 `800KB`，远大于 `4 * 64KB`

所以 `500` 行场景复用收益明显，而 `2000` 行场景复用收益不稳定。

## 3. 设计目标

1. sizing 必须按 `batchSizeByRow` 的目标规模算，而不是只看眼前这批数据有多少行；
2. 同一类 workload 下，buffer 规格要很快稳定下来；
3. 行为要能靠单元测试直接断言；
4. payload 内容和现有序列化语义保持不变；
5. 避免“自学习状态”带来的解释成本和 bug 风险。

## 4. 非目标

这版设计**不做**下面这些事情：

1. 不做机器学习；
2. 不做滚动分位数学习；
3. 不做带历史窗口的自适应控制；
4. 不做 per-field hard cap；
5. 不改 dedicated chunk 的语义；
6. 不改 fixed-width 字段的存储方式。

## 5. 当前实现约束

这个设计依赖当前 WSEW 已有的执行模型：

- WSEW 在真正建 column buffers 之前，已经把当前 batch 的 rows 取成了
  `List<Map<Integer, Column>>`
- `triggerSerializeProgressive(...)` 只有在 `writeQueue` 非空时才会提交任务
- `batchSize = min(writeQueue.size(), batchSizeByRow)`，所以 progressive batch 至少有 `1` 行
- `ColumnarWSEWSerializationTask.compute()` 会严格取满 `batchSize` 行，再进入 build path

因此：

- `observedRows == 0` 在这条路径里不是一个真实运行分支
- 这次设计里不需要再保留 zero-row fallback

## 6. 新的核心抽象：`BufferSpec`

对每个变长列，引入一个明确的规格：

- `chunkBytes`
- `reusableChunkCount`

这两个值共同决定该列的 reusable value buffer 形态。

设计原则很简单：

- **规格相同**：复用
- **规格不同**：整列重建

不做“在线扩容一点、缩容一点”的中间态逻辑。

## 7. Batch-local profiling

在建变长列 buffer 之前，先扫一遍当前 batch 的 rows。对每个变长列统计：

- `observedRows`
- `observedValueBytes`
- `maxSingleValueBytes`
- `nonNullRows`

这里的 `observedValueBytes` 必须是**编码后真正会写进 value area 的字节数**，而不是
Java 对象个数。

这个 profiling 只看当前 batch，不保留历史状态。

## 8. 按 `batchSizeByRow` 投影到满批规模

即使当前只拿到了半批数据，也要按满批规模去估算。

公式：

```text
projectedValueBytes =
    max(
        observedValueBytes,
        ceil(observedValueBytes * batchSizeByRow / observedRows)
    )
```

含义是：

- 如果当前就是满批，那就是实际值；
- 如果当前是半批或更小，就按“当前每行平均 value bytes”放大到满批；
- 这样 sizing 决策不会被短 batch 误导得过小。

## 9. `chunkSize` 到底怎么计算

这是这次设计最重要的点。

### 9.1 先算“理想每 chunk 该承载多少字节”

我们先固定一个目标：希望一列在正常情况下，大致落在 `4` 个 active chunks 左右。

```text
targetActiveChunks = 4
perChunkTarget = projectedValueBytes / targetActiveChunks
```

### 9.2 再保证 chunk 不能比最大单值还小

如果某一行单值本身就很大，那么 chunk 至少要不小于这个单值下限：

```text
chunkCandidate = max(maxSingleValueBytes, perChunkTarget)
```

### 9.3 最后再做桶化

为了匹配 Netty pool，同时避免每批都因为几个字节差异换规格，最终的 chunk size
不是精确值，而是桶化后的档位：

```text
idealChunkBytes =
    clamp(
        roundUpPow2(chunkCandidate),
        8KB,
        64KB
    )
```

也就是：

1. 先向上取到 2 的幂；
2. 再夹在 `8KB ~ 64KB` 之间。

### 9.4 为什么是这几个桶

这里的 `8 / 16 / 32 / 64KB` 不是“写死不变的工作负载值”，而是**固定的分配档位**。

真正动态变化的是：

- 每批算出来的 `chunkCandidate`
- 它最终落到哪个桶

保留桶化而不是用精确字节值，有三个原因：

1. **更匹配 pooled allocator**
2. **避免微小波动导致频繁换规格**
3. **测试容易写**，因为输出只会落在少数确定档位里

### 9.5 两个例子

#### 例子 1：`projectedValueBytes = 200KB`

```text
targetActiveChunks = 4
perChunkTarget = 200KB / 4 = 50KB
maxSingleValueBytes = 400B
chunkCandidate = max(400B, 50KB) = 50KB
roundUpPow2(50KB) = 64KB
idealChunkBytes = 64KB
```

#### 例子 2：`projectedValueBytes = 800KB`

```text
targetActiveChunks = 4
perChunkTarget = 800KB / 4 = 200KB
maxSingleValueBytes = 400B
chunkCandidate = max(400B, 200KB) = 200KB
roundUpPow2(200KB) = 256KB
clamp(256KB, 8KB, 64KB) = 64KB
idealChunkBytes = 64KB
```

这里虽然目标算出来更大，但因为上限是 `64KB`，所以最终 `chunkSize` 还是 `64KB`。
这时候剩下的容量需求不再靠变大 `chunkSize` 解决，而是靠**增加 reusable chunk 数**解决。

## 10. `reusableChunkCount` 如何计算

在 `idealChunkBytes` 算出来之后，再算需要覆盖多少个 reusable chunks：

```text
requiredReusableChunks =
    ceil(projectedValueBytes / idealChunkBytes)
```

这一步是关键，因为它把现在固定常量 `4` 替换掉了。

例如：

- `200KB / 64KB -> 4`
- `800KB / 64KB -> 13`

所以对于 `2000` 行的大 batch，系统会自然从 “4 个 chunk” 提升到 “约 13 个 chunk”，
而不是被固定常量卡住。

## 11. 复用 / 重建规则

这版规则故意保持得非常硬：

- `currentSpec == wantedSpec`：复用
- `currentSpec != wantedSpec`：整列重建

也就是说，不做：

- 原地增减 chunk 数
- 原地替换 chunk size
- 缓慢扩容 / 缓慢缩容

这样做的原因：

1. 行为完全确定
2. 容易断言“这批该复用还是该重建”
3. 不会因为历史大 batch 把 oversized cache 一直留下来
4. 出问题时容易定位

在前面的“按满批投影”规则下，稳定 workload 的 `wantedSpec` 会很快稳定，所以不会造成
频繁重建。

## 12. Dedicated chunk 语义不变

单个值特别大时，仍然沿用当前 dedicated chunk 语义。

这次设计只改三件事：

1. 变长列 `chunkSize` 怎么算
2. 变长列 `reusableChunkCount` 怎么算
3. 当前 buffer 是复用还是重建

不改 payload 格式，不改值编码，不改 dedicated chunk 的判断方向。

## 13. 组件改动建议

### 13.1 `WSEWColumnPreparedStatement`

新增一个 batch-local profiler，负责：

1. 扫描当前 rows
2. 对每个变长列算出 `projectedValueBytes`
3. 对每个变长列生成 `wantedSpec`
4. 决定每列是复用还是重建
5. 再把真实 rows 填进选定的 buffers

### 13.2 `Stmt2ColumnFieldBuffer`

需要暴露当前 reusable value buffer 的规格信息，至少包括：

- 当前 `chunkBytes`
- 当前 `reusableChunkCount`

这样上层才能做 `currentSpec == wantedSpec` 判断。

### 13.3 `ReusableChunkedBuffer`

构造时直接接收：

- `chunkBytes`
- `reusableChunkCount`

不做在线 resize；重配置通过“整列重建”完成。

## 14. 错误处理和不变量

1. 缺列仍然按当前逻辑抛 `SQLException`
2. `observedRows == 0` 不是这条路径里的真实分支，不保留 fallback
3. 如果某列本批没有非空值，也仍然按公式得到一个确定的 `wantedSpec`
4. 如果重建后在填充过程中失败，要释放新建 buffer，并保持异常传播语义不变

## 15. 测试设计

### 15.1 单元测试

1. **半批投影**
   - 输入：`observedRows < batchSizeByRow`
   - 断言：`projectedValueBytes` 按满批投影

2. **chunk size 桶化**
   - 输入一组固定 `projectedValueBytes / maxSingleValueBytes`
   - 断言最终 `chunkBytes` 桶位正确

3. **chunk count 推导**
   - 断言 `requiredReusableChunks = ceil(projectedValueBytes / idealChunkBytes)`

4. **稳定 workload 复用**
   - 第一批建 buffer，后续同规格 batch 持续复用

5. **规格切换重建**
   - 小 workload 后接大 workload
   - 断言 `wantedSpec` 变化时整列重建

6. **payload 一致性**
   - 复用路径和重建路径对同一批 rows 产出的 payload 完全一致

### 15.2 回归测试

至少保证下面几组不退：

- `WSEWColumnPreparedStatementTest`
- `ReusableChunkedBufferTest`
- `Stmt2ColumnBindSerializerTest`
- `WsEfficientWritingTest`

## 16. 风险

1. 当前 batch 如果代表性很差，按满批投影仍可能不完全精准  
   这是可以接受的，因为本设计优先追求确定性而不是历史学习。

2. 不设 per-field hard cap，意味着超大字段可能会缓存更多 reusable chunks  
   这是这版设计的明确取舍。

3. 精确规格匹配才复用，可能比“带迟滞的自适应”重建更频繁  
   但它显著更容易测试和解释。

## 17. 推荐落地顺序

第一步只实现这版**确定性、无学习状态**的设计。

如果后续真实 benchmark 证明“规格切换重建”仍然太重，再考虑第二阶段：

- 引入最小状态的 `lastProjectedSpec`
- 只在极少数情况下放宽“精确匹配才复用”的规则

在那之前，不建议先上真正的 learner。
