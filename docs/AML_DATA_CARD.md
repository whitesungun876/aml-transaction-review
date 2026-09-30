# IBM AML 数据卡

## 来源与冻结

- 来源：[IBM AML-Data 官方入口](https://github.com/IBM/AML-Data) 所链接的 [Kaggle 发布](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)。
- 发布版本：8；API 元数据更新时间 2025-07-08。
- 文件：HI-Small_Trans.csv；475,664,283 bytes。
- SHA-256：`b19d39f515523373f991b689c07e11e7b0b95c17a2c27a87d91584ae16c5b040`。
- 下载与核验日期：2026-09-30。哈希是本次下载的完整性身份，不是数据提供方的签名背书。
- API 对所选版本标注：Community Data License Agreement — Sharing — Version 1.0。原始数据和逐条结果默认仅本地保存。
- 原始元信息与清单：`.runtime/raw/provider_metadata.json`、`.runtime/raw/source_manifest.json`。

## 实测数据

- 原始 CSV 记录：5,078,345；接受：5,078,345；格式隔离：0。
- 观察范围：2022-09-01 00:00（含）至 2022-09-11 00:00（不含）。
- 范围内：5,077,237；正例：4,522。
- 范围外：1,108；其中正例：655。该范围外统计并不等于“全部是洗钱”，不要照搬提供方对某些尾部记录的概括。
- 观察范围在模型训练前，依据提供方说明的主要交易期确定；没有依据测试分数调整。
- 时间精度分钟，未声明时区，保持 naive timestamp。
- 原始表头有两个 `Account` 列。解析必须按完整、精确表头进行位置映射，不能直接用普通 DictReader。
- 不存在已核实的唯一业务交易 ID；当前 ID 使用源文件 SHA-256 与不含表头的 1-based CSV 逻辑记录序号。

## 可解释范围

合成数据是整个模拟金融生态的交易视图，不等同于单家银行实际可见的信息。账户历史采用整个选定数据文件的可见范围，因此只能用于该离线环境的研究。标签是交易级模拟洗钱标签，不是独立案件或经过司法认定的事实。

金额保留原始币种，不假设汇率。模型分数不是校准后的洗钱概率。账户和场景答案不直接进入模型特征。公开 CI 使用自行构造的样例，不再分发这些记录。

## 时间协议

2022-09-01 为历史预热；09-02 至 09-07 前训练；09-07 至 09-09 前验证；09-09 至 09-11 前测试。5/2/2 完整日替代精确 60/20/20，以对应逐日审核预算，已在训练前写入配置。

对应质量证据：`.runtime/prepared-v1/quality_report.json` 与 `COMPLETE.json`。实际完整切分见 `.runtime/experiment-v1/split_manifest.json`，全量重建哈希见 `reports/feature_reproducibility.json`。
