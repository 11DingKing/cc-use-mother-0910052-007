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

## 回测不可变快照

同名回测在不同时间运行会读到变化后的行情、策略与费用参数，导致报告无法复核。
为此，每次回测运行都绑定一份**不可变输入快照**，快照在创建时冻结并分别哈希：

- 行情片段（K线序列，`candles_hash`）
- 策略版本与信号集合（`strategy_version`、`signals_hash`）
- 交易日历（`calendar_hash`）
- 费用口径（手续费、滑点、仓位、初始资金，`fee_hash`）

规则：

- 请求参数在快照创建时完成规范化（周期别名如 `d/1d/day` 归一为 `daily`），
  同一账户下相同语义输入永远映射到同一 `snapshot_id`，不同输入绝不共用快照；
- 数据补录只影响之后创建的新快照，既有快照与运行不受任何影响；
- 失败后继续（`resume`）、取消后重试都从原快照的断点恢复；
  同快照重放（`replay`）产生全新运行但输入完全一致，结果逐位相同；
- 并发启动通过 `idempotency_key` 幂等去重，同账户同键只会有一个运行，
  且该键绑定的快照不可更换；
- 所有快照/运行查询按 `account_id` 隔离，跨账户访问一律返回 404；
- 已完成的运行可归档（`archive`），归档后只读，不可取消或续跑；
- 运行报告通过 `inputs_locked` 明确列出被锁定的输入及各分量哈希，
  通过 `completeness.is_complete` 声明结果是否完整（已处理K线数/应处理K线数及原因）。

### API

```
POST /api/backtest/snapshots                 创建（或复用）快照
GET  /api/backtest/snapshots?account_id=     列出快照（账户隔离）
GET  /api/backtest/snapshots/{id}?account_id=
POST /api/backtest/runs                      启动运行（绑定快照，同步执行）
GET  /api/backtest/runs?account_id=          列出运行（账户隔离）
GET  /api/backtest/runs/{id}?account_id=     运行报告（锁定输入 + 完整性）
POST /api/backtest/runs/{id}/resume?account_id=   失败后继续 / 取消后重试
POST /api/backtest/runs/{id}/replay?account_id=   同快照重放
POST /api/backtest/runs/{id}/cancel?account_id=   请求取消
POST /api/backtest/runs/{id}/archive?account_id=  归档结果
```

旧的 `POST /api/backtest/run` 接口内部同样走快照机制（使用 `default` 账户）。
