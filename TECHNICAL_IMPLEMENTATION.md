# AML Transaction Review — 技术实现方案

- 版本：v1.0
- 日期：2026-09-30
- 状态：原始设计基线；本地已实施部分的命令见 [运行手册](docs/AML_RUNBOOK.md)，逐阶段结果见 [验收台账](ACCEPTANCE.md)。本地与云端已完成验收；此文仍保留原设计，完成范围以验收台账为准。
- 需求基线：[PRD](PRD.md)
- 现有工程：[ERP Risk MLOps](https://github.com/whitesungun876/erp-risk-mlops)

## 1. 实现原则与现状

现有 ERP `Document`、六维特征、训练模型和 `protocol.py` 对 ERP 数据语义有明确依赖，不能把交易 CSV 填进原对象后直接使用。通用 `ScoredUnit` / `evaluate_budgets` 可以作为首个复用边界。

参考代码：

- [ERP protocol.py](https://github.com/whitesungun876/erp-risk-mlops/blob/main/src/erp_risk/protocol.py)
- [通用预算评估](https://github.com/whitesungun876/erp-risk-mlops/blob/main/src/erp_risk/evaluation.py)
- [现有批处理契约](https://github.com/whitesungun876/erp-risk-mlops/blob/main/src/erp_risk/batch.py)

实施前记录所用代码 commit，检查本地目标 checkout、对应 AGENTS.md 和未提交更改。不要根据本文件位置推断它就是要修改的 ERP 仓库。

原则：新增独立 `aml_risk`，保留 ERP 命令、数据、实验配置、输出表和模型；本轮不进行大型公共框架重构。

## 2. 目标架构

```text
本地 IBM 原始文件 + source_manifest
  ↓ adapter / schema checks
标准交易 Parquet ─────────── 独立 labels Parquet
  ↓ temporal feature builder          ↓ 仅训练或评估连接
特征快照 + feature_manifest
  ↓ frozen temporal split
训练 / 验证 / 测试
  ↓ rules + XGBoost
版本化模型、预处理器、特征签名
  ↓ local batch scoring
scores + review queue + run_manifest
  ↓ evaluation / reconciliation
汇总指标、逐日结果、质量报告、错误分析

可选：明确授权后，将同一契约接入 UC 模型与 Delta 输出表
```

先交付本地 CLI 与文件制品，不增加服务 API 或前端。本地结果可用 Parquet 保存；汇总报告用 JSON/Markdown。新增依赖必须依据实际 Python 环境锁定，不沿用未经验证的版本猜测。

## 3. 源清单与数据契约（A01–A03）

### 3.1 source_manifest

至少包含 `dataset_name`、`release_id`、`source_url`、`retrieved_at`、`files[path,size,sha256]`、`license_reference`、`schema_mapping_version`、`time_semantics`、`subset_policy`。

`dataset_version` 由规范化来源清单的内容哈希确定。下载时间不应使同一份文件在相同语义下获得不同内容身份；元信息与内容身份字段分开定义。

### 3.2 标准 TransactionRecord

| 字段组 | 拟议字段 | 规则 |
|---|---|---|
| 身份 | transaction_id、dataset_version | 若原始 ID 不可靠，使用 file_sha256 + 原始 CSV 记录序号；不声称是银行业务 ID |
| 时间 | event_time、原始时间文本 | 保持原始精度；未说明时区则标明 unknown，不自动按 UTC 解读 |
| 账户 | sender_bank、sender_account、receiver_bank、receiver_account | 以银行与账户组合建立账户键，防止不同银行同号碰撞 |
| 金额 | payment_amount/currency、received_amount/currency | 原始精度用 Decimal；进入特征后按签名转换；禁止跨币种直接相加 |
| 分类 | payment_format | 已知映射及 unknown 行为明确 |
| 证据 | source_file_sha256、source_record_number | 指向原始输入；包含表头、多行 CSV 的定位约定须有测试 |

实际原始列名、是否有唯一 ID 和哪些字段必需，需要在 M0 读取数据后确认。标准字段缺失时不得凭空生成业务事实。

标签保存为独立 `transaction_id -> label` 表。训练／评估显式连接；评分接口不接受标签列。模拟模式、场景标签、案件答案字段一律不进入特征白名单。

### 3.3 质量处理

- 文件级错误：哈希不符、缺少必需列、映射版本不匹配，立即失败。
- 行级错误：记录错误代码、来源位置、字段名；日志不输出整条交易。
- 正式默认严格模式：出现隔离记录时不继续训练。若需容忍，必须预先配置阈值与业务理由，并报告排除分布。
- 文件哈希与行序号确定的 ID 可用于重放；内容重复只标记，不推断交易重复。
- 金额符号、零值、标签可取值及缺失处理以所选版本说明为准。
- 保证输入、接受、隔离数量对账及接受记录 ID 唯一。

## 4. 历史特征算法（A04）

### 4.1 时间语义

按 event_time 排序，历史窗口使用 `[t - window, t)`。相同时间戳的所有交易先读取历史状态生成特征，再统一加入状态；ID 仅用于稳定排序，不用于虚构时间先后。

增量模式必须保存历史状态版本和已处理边界；第一版优先整段重放以降低状态恢复复杂度。验证／测试可以使用此前已经发生的无标签交易历史，不能用其标签或重新拟合预处理器。

### 4.2 第一版特征清单

- 当前交易：金额变换、币种与支付方式类别、是否跨银行。
- 发送方历史：1 小时／24 小时交易数、不同交易对手数量。
- 同币种历史：金额统计及当前交易相对历史金额水平。
- 关系：当前接收方是否为发送方历史中首次出现。
- 冷启动：历史是否为空、有效历史长度、分母不足标记。

具体窗口基于实际时间覆盖冻结。分母为零或历史不足时使用显式缺失标记，不用无穷大。金额变换前的负数处理必须按已确认的数据契约执行。

原始账户 ID 不直接入模；ID 只用于分组、回溯与误差分析。跨币种金额不合并；不凭空添加汇率或国别信息。

### 4.3 可验证性

保留慢速参考实现，在小样例上与优化实现逐字段比较。未来扰动不变性、同时间戳置换不变性、窗口端点、跨银行同账户号、多币种与冷启动均进入测试。

预处理器仅在训练集拟合，未知类别处理固定。模型签名记录列名、顺序、类型、窗口、缺失策略、预处理器版本与时间语义。

## 5. 切分与实验协议（A05、B01–B02）

### 5.1 冻结顺序

1. 获取并检查数据，确定连续观察范围与历史预热区间。
2. 在查看模型表现前冻结时间边界、特征和主要指标。
3. 初始建议按时间跨度 60/20/20 划分，最终配置保存精确时间而非仅比例。
4. 相同时间戳不能跨分区；预热记录用于历史，不计入指标。
5. 输出各分区 ID 哈希、时间范围、条数及标签支持度。
6. 若存在无正例或支持不足，明确暂停或降为探索性实验；扩大范围须产生新的协议版本。

不得通过反复试验测试分数移动边界。测试集只用于冻结后的最终评估；若根据测试错误修改模型，原测试集转为开发资料，另设新留出集或如实标注探索性结果。

### 5.2 基线与模型

- 规则：训练侧统计阈值形成可解释排序；不冒充监管阈值。
- XGBoost：固定随机种子、小规模候选参数；验证集选型。
- 类别不平衡处理仅在训练侧进行；测试不重采样。
- 类别编码、缺失策略及模型一同保存。
- Isolation Forest、SHAP、额外滚动窗口为可选，不影响最低交付。

### 5.3 指标与统计边界

通用 `evaluate_budgets` 分别用于全测试集和每日分组，保持同一套分数与预算。排序固定为 `score desc, transaction_id asc`；记录并列边界。

比例预算 `K = ceil(N × fraction)`，报告每日实际 K，说明小批次上取整的影响。缺少正例时 AP 和 Recall 不可定义。跨日汇总分别给出总命中／总审核数与每日分布，不混淆宏平均和微平均。

Average Precision 明确为采用的离散排名定义，不自动与梯形 PR-AUC 混称；若追加其他实现，必须单独命名并测试。规则并列分数多时报告并列敏感性，不把 ID 顺序带来的优势当模型能力。

同一账户和模拟过程的相关性限制了独立性。第一版不使用逐交易独立 bootstrap 宣称显著优越；优先报告日期切片和原始计数。有充分时间块时才考虑分块不确定性分析，并说明块间相关性局限。

## 6. 模型与运行身份（B03–B04）

### 6.1 本地制品

`model_manifest` 包含模型内容哈希、训练 split 哈希、特征签名、预处理器哈希、参数、依赖版本与代码 commit。

`batch_manifest` 绑定输入文件哈希、交易范围、feature_version、预处理配置与时间边界。显式 batch_id 如被重复使用，必须核对清单哈希；不一致即拒绝。

评分输出至少包含 dataset_version、batch_id、transaction_id、feature_version、model_id、model_version、score、score_type、rank、reason_codes、source_reference。运行时间等非确定字段放在独立 run_manifest，不参与业务结果相等比较。

### 6.2 幂等规则

- 相同输入、特征与模型：结果数量、唯一键及业务内容一致。
- 同一个 batch_id 不得对应不同 batch_manifest。
- 不同模型版本并存，不覆盖旧评分。
- 本地先写临时制品，完整校验后原子发布；失败不留下“成功”标记。
- 同一机器学习分数的浮点比较容差须显式声明；ID、排序与计数要求精确一致。

### 6.3 可选 Databricks 接入

需单独确认费用与目标后执行：

- 新建 AML 命名空间模型、实验与表，不改 ERP 资源。
- 每批解析一次 UC alias，固定数值版本后加载并校验特征签名。
- 独立结果表复合键建议为 `(batch_id, model_id, model_version, transaction_id)`。
- 写入前校验 batch_manifest 冲突；通过后使用 Delta MERGE。
- 第一版单写入者、串行作业；不声称已解决任意并发写入下的全局去重。
- 验证输入数、输出数、唯一键数，实际重跑至少一次，并保存运行证据。
- 不自动加 schedule，不把模拟的 Spark／Delta 单元测试当作真实云验收。

## 7. 接口与文件布局

拟议接口（类型名称在实现中可微调，职责不变）：

```text
validate_source(files, manifest) -> SourceValidation
read_transactions(source, mapping) -> transactions, labels, QualityReport
build_features(transactions, feature_config) -> features, FeatureManifest
split_by_time(features, frozen_protocol) -> SplitManifest
train_ranker(train, validation, model_config) -> ModelBundle
score_batch(features, model_bundle, batch_manifest) -> scores, RunManifest
evaluate_scores(scores, labels, budget_config) -> EvaluationReport
```

拟议布局：

```text
src/aml_risk/{schema,adapter,features,protocol,rules,model,batch}.py
configs/aml_experiment.json
scripts/{prepare_aml,train_aml,score_aml}.py
tests/test_aml_{adapter,features,protocol,model,batch,evaluation}.py
tests/fixtures/aml_synthetic/       自行构造，不复制 IBM 原始记录
docs/AML_DATA_CARD.md
docs/AML_RUNBOOK.md
reports/AML_EXPERIMENT.md
```

新增 CLI 约定支持显式输入、输出与配置路径；默认不联网、不上传、不创建云资源。输出目录已存在且不属于同一运行时拒绝覆盖。这里不提供可被误认为已存在的执行命令。

## 8. 测试矩阵

| 层级 | 场景与操作 | 预期 |
|---|---|---|
| 文件 | 改变原始字节但沿用哈希 | 导入失败 |
| 契约 | 缺列、非法金额、无法解析时间 | 有明确错误及来源；按严格模式停止 |
| 对账 | 混合有效／无效行 | 输入 = 接受 + 隔离 |
| 身份 | 不同银行同账户号、同内容不同源行 | 不合并不同账户，不擅自删除交易 |
| 特征 | 修改未来记录 | 历史特征不变 |
| 特征 | 打乱同时间戳记录 | 相同特征与评分 |
| 特征 | 手算窗口样例对照 | 优化实现与参考实现一致 |
| 切分 | 边界相同时间戳、重叠 ID | 正确分组或拒绝非法 split |
| 标签 | 将标签、场景答案列传入评分 | 拒绝输入 |
| 模型 | 列序错误、未知类别、加载制品变更 | 签名检查生效，未知值按冻结策略处理 |
| 评估 | 零正例、极小批次、并列分数 | 指标定义正确，分母和并列影响可见 |
| 批处理 | 同批重跑、冲突 batch_id | 不增重复；不同清单拒绝复用 |
| 故障 | 写入中途失败 | 无有效完成标记；可安全重试 |
| 兼容 | 执行原 ERP 受影响测试 | 不退化，不重写旧报告 |
| 云验收 | 如获授权，实际写入并重跑 | 真实计数、唯一键、版本读回通过 |

测试数量由覆盖需求决定，不预先虚构“新增 N 项测试”。CI 只处理自造样本和本地测试，不依赖云凭据。

## 9. 资源、安全与降级

- 执行前检查文件大小、内存和磁盘；正式范围超过本机预算则暂停或采用预先声明的连续时间子集。
- 读取可分块，但历史特征需维持完整的前序窗口；不能通过随机抽行获得错误的账户历史。
- 第一版避免将未知规模数据整体 `.toPandas()`；仅在资源预检明确可容纳时使用内存实现。
- 秘密仅通过现有安全配置读取；不放入 CLI 参数、报告或日志。
- 质量报告公开部分只放汇总与自造样例；逐条证据默认本地保存。
- 数据下载受阻时，可以完成适配器的合成样例测试，但不得宣称 IBM 数据实验已完成。
- 云环境不可用时交付本地闭环，并明确云端未验收，不伪造成功截图或运行 ID。
- 模型指标不佳时保留规则基线及失败分析，不通过测试泄漏追求高分。

## 10. 任务依赖与交付检查

| 任务 | 对应需求 | 前置依赖 | 产出 |
|---|---|---|---|
| T0 目标 checkout 与数据核验 | A01、A06 | 无 | 代码快照、数据清单、字段说明 |
| T1 标准契约与适配器 | A02、A03 | T0 | 数据／标签分离及质量测试 |
| T2 时间特征参考实现 | A04 | T1 | 手工样例、未来扰动测试 |
| T3 协议冻结 | A05 | T1、T2 | 精确切分与特征签名 |
| T4 模型与评估 | B01、B02 | T3 | 可复现实验与错误分析 |
| T5 批处理输出 | B03、B04、B05 | T3；最终验收需 T4 | 制品身份、幂等及来源追溯 |
| T6 回归与交付 | B06、A06 | T4、T5 | Runbook、完整测试结果、报告 |
| T7 可选云验收 | 可选增强 | T6 + 明确授权 | 云端运行与读回证据 |

T5 的契约开发可与 T4 并行，但不能在模型与特征签名冻结前宣称联调完成。

完成定义：PRD 阶段 A/B 验收逐项有证据，ERP 回归无退化，所有未验证状态明确标注。只有完成 T7 才能声称 AML 云端批处理已验证；任何公开发布或简历更新属于后续单独操作。
