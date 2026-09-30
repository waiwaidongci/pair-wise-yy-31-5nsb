# 车辆安全召回与修复跟踪系统

标准库 Python 3.11+ + SQLite。支持召回草稿、监管审核发布、范围按版本调整、车辆登记与跨境流转、维修网点零件库存、修复证据复核、未完成高风险车辆统计，以及通知和监管上报版本。

## 五账对账

召回范围、车辆流转、网点库存、维修记录、监管上报通过统一对账视图衔接：

- `GET /api/reconciliation`：全部已发布召回的对账汇总。
- `GET /api/recalls/{id}/reconciliation`：单个召回的完整对账结果。
  - `vehicles`：每台车的当前范围版本、是否在范围、待办、维修历史、欠件挂起（流水号/重试次数）、通知版本、**最近重算原因**。
  - `dealers`：每个网点的可用库存、本国待办需求、预计缺口、欠件提交明细、台账结余、**最近重算原因**。
  - `reports` / `recompute_runs`：监管上报各范围版本与重算流水。
  - `checks`：库存账实一致、每笔消耗都有维修单、当前范围通知无遗漏、上报覆盖全部版本、最新上报数量已重算、历史可追溯。

## 重算规则

- **范围或所在国一变就重算**：监管发布、范围调整（新版本）、车辆登记、跨境转手都会对相关已发布召回按当前 `scope_version` 重算受影响待办、通知与监管上报。
- 通知只对当前在范围内车辆补当前版本（`INSERT OR IGNORE`），**历史版本通知继续保留可查**；监管上报按 `(召回, 范围版本)` 原地刷新为最新受影响清单并重新排队。
- **已确认维修不受范围变化影响**：车辆后续出范围，confirmed 维修与历史通知仍可在对账视图和 `/api/recalls/{id}` 查到。
- 每次重算写入 `recompute_runs`（触发类型、原因、受影响数量、操作人），页面和接口都能看到重算原因。

## 备件与维修的一致性

- 维修提交在单个事务内执行：先对备件行做**条件扣减**（`UPDATE ... WHERE available>=1`）且命中一行后，才插入维修单与消耗台账，否则整笔回滚。
- 服务层写锁串行化"先查后写"：两笔维修并发抢最后一箱时，只有一台车成功；**不会出现扣了库存却没有维修单**。
- 库存不足时提交内容原样保留在 `repair_attempts`（欠件挂起，出现在对账视图），备件到货后用**同一流水号** `idempotency_key` 重试即可；成功流水号重放返回原维修单，不重复扣库存；复核 flag 退回的备件回写台账。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

默认端口 `8213`。身份通过 `X-Actor` 与 `X-Role` 请求头模拟，角色为 `manufacturer`、`regulator`、`dealer`。可用 `--port`、`--db` 覆盖。首页 `/` 为按车辆/网点展示当前范围版本、欠件与重算原因的对账台。

## 主要接口

- `POST /api/dealers`、`POST /api/vehicles`：登记网点和车辆（车辆登记即触发已发布召回重算）。
- `POST /api/vehicles/{vin}/transfer`：更新车辆所在国家和车主（所在国一变即重算）。
- `POST /api/recalls`、`POST /api/recalls/{id}/submit`：创建并提交召回。
- `POST /api/recalls/{id}/review`：监管发布或退回（发布即按初始版本重算）。
- `POST /api/recalls/{id}/scope`：调整召回范围并生成新版本，重算待办/通知/上报。
- `POST /api/recalls/{id}/parts`：维修网点入库。
- `POST /api/repairs`、`POST /api/repairs/{id}/review`：报告并复核维修（原子扣减、欠件保留、流水号重试）。
- `GET /api/reconciliation`、`GET /api/recalls/{id}/reconciliation`：对账结果。
- `GET /api/recalls/{id}/unfinished`：查看高风险未完成车辆。
- `GET /api/state`、`GET /api/health`：状态与健康检查。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

`tests/test_reconcile.py` 额外覆盖：跨境转手后按最新范围补发通知/刷新上报且历史保留、两车并发抢最后一箱只有一台成功、失败提交保留并按原流水号重试、网点欠件与缺口视图、范围变更重算原因。

当前为本地原型：跨境规则用许可字符串模拟，零件库存与维修记录是简化模型，不包含真实 VIN 解码、监管接口、物流系统或法定通知渠道。
