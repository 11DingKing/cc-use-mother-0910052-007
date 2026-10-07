# 回测复现快照业务服务

这是一个使用 Python、FastAPI 与 SQLite 实现的纯后端业务服务，包含领域模型、数据访问、业务编排、接口和异常路径测试。项目可在单个 Linux 应用容器内离线运行，使用本地 SQLite 或内存替身，不依赖外部运行服务。

## 安装

```bash
python3 -m pip install -r requirements.txt
```

## 测试

```bash
python3 -m pytest -q
```

## 构建检查

```bash
python3 -m compileall -q app
```

## API 导入冒烟

```bash
python3 -c "from app.main import app; print(len(app.routes))"
```

## 启动

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 不可变输入快照（回测可复现）

同一名称的回测在不同时间运行，可能读到补录后的行情、升级后的策略或调整后的
费用参数，导致历史报告无法复核。为此所有回测都先封存一份**不可变输入快照**
（`app/snapshot/`），把四类输入关联到一次运行：

| 输入 | 锁定内容 |
| --- | --- |
| 行情片段 `market` | 实际参与计算的每根 K 线（OHLCV）及逐根内容哈希、来源、声明区间与实际覆盖区间 |
| 策略版本 `strategy` | 缠论流水线/回测引擎的**代码版本指纹** + 归一化策略参数 + 检测器链 + 别名映射 |
| 交易日历 `calendar` | 实际参与计算的交易日集合及哈希（与行情片段绑定） |
| 费用口径 `fees` | 初始资金、仓位、手续费率、滑点、费用模型及哈希 |

快照 ID 由四类输入的内容指纹派生（`snap-<账户>-<指纹>`），同账户同输入必然
得到同一快照；任一根 K 线被补录/修订或任一参数变化都会产生**新快照**，旧快照
及其归档结果永不改变。

隔离与生命周期：

- 所有接口要求 `X-Account-Id` 头，快照/运行/归档按账户目录隔离，跨账户访问返回
  403；因此数据补录、参数别名、并发启动、取消后重试、跨账户查询都不会串用快照。
- 参数别名（`手续费率`/`commission_rate`/`comm`、`滑点`、`本金` 等）在入快照前
  归一化；同一规范参数被赋予冲突值会被拒绝（409）。
- 构建分 `market/strategy/calendar/fees` 四阶段落盘，某阶段失败后可对同一
  `build_id` 调 `/builds/{id}/seal` 断点续建，已完成阶段不重跑。
- 运行状态机 `pending → running → completed/failed/cancelled`：
  - 失败或取消后对同一 run、同一快照调 `/runs/{id}/retry` 继续（`attempts+1`）；
  - 调 `/runs/{id}/replay` 用同一快照发起一次新运行，结果与首次运行逐值一致；
  - 取消为协作式（`/runs/{id}/cancel` 打标记，引擎在每根 K 线前的取消点停止）；
  - 并发启动用 `idempotency_key` 去重，同一键只创建一个 run，且不得跨快照复用。
- 结果与报告一次性归档到 `BACKTEST_ARCHIVE_DIR`（默认 `data/archive/`，可用环境
  变量覆盖），已归档结果拒绝覆写；关系库仅作副本，写入失败不影响归档。
- 报告含 `snapshot.locked_inputs`（明确列出被锁定的四类输入）与 `completeness`
  （`complete/partial/unverified` 及缺失区间明细），说明结果是否覆盖完整区间。

典型接口（均在 `/api/backtest` 前缀下）：

```
POST /run-snapshot                 # 采集→封存→执行一步到位（头 X-Account-Id）
POST /snapshots                    # 只构建并封存快照
POST /builds/{build_id}/seal       # 失败后续建并封存
POST /snapshots/{id}/runs          # 基于锁定快照启动（可带 idempotency_key）
POST /runs/{id}/execute            # 执行；已完成则回放归档报告
POST /runs/{id}/cancel             # 请求取消
POST /runs/{id}/retry              # 失败/取消后继续
POST /runs/{id}/replay             # 同快照重放（新 run）
GET  /runs/{id}/report             # 归档报告（含锁定输入与完整性）
GET  /snapshots/{id}               # 查看快照锁定了哪些输入
```

旧的 `POST /api/backtest/run` 内部已改为走同一快照流程（归入 `default` 账户）。

