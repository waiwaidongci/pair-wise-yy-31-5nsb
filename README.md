# 车辆安全召回与修复跟踪系统

标准库 Python 3.11+ + SQLite。支持召回草稿、监管审核发布、范围按版本调整、车辆登记与跨境流转、维修网点零件库存、修复证据复核、未完成高风险车辆统计，以及通知、待办和监管上报的版本化与对账。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

默认端口 `8213`。身份通过 `X-Actor` 与 `X-Role` 请求头模拟，角色为 `manufacturer`、`regulator`、`dealer`。可用 `--port`、`--db` 覆盖。

## 主要接口

- `POST /api/dealers`、`POST /api/vehicles`：登记网点和车辆。
- `POST /api/vehicles/{vin}/transfer`：更新车辆所在国家和车主，并触发受影响召回的重算。
- `POST /api/recalls`、`POST /api/recalls/{id}/submit`：创建并提交召回。
- `POST /api/recalls/{id}/review`：监管发布或退回。
- `POST /api/recalls/{id}/scope`：调整召回范围并生成新版本通知/待办/上报。
- `POST /api/recalls/{id}/parts`：维修网点入库。
- `POST /api/repairs`、`POST /api/repairs/{id}/review`：报告并复核维修。
- `GET /api/recalls/{id}/unfinished`：查看高风险未完成车辆。
- `GET /api/recalls/{id}/reconciliation`：召回范围、车辆流转、网点库存、维修记录、监管上报的统一对账结果。
- `GET /api/state`、`GET /api/health`：状态与健康检查。

## 对账与重算

- **对账**：`GET /api/recalls/{id}/reconciliation` 把当前召回范围、车辆（所在国 / 原籍国 / 在范围内 / 当前范围版本 / 已通知版本 / 维修状态 / 待办 / 是否欠件）、网点（备件库存 / 待办数 / 欠件数 / 欠件车辆）、监管上报和重算原因接成一份结果。
- **重算**：范围调整（`scope_changed`）、车辆转手（`vehicle_transferred`）、车辆登记（`vehicle_registered`）或召回发布（`recall_published`）后，自动重算受影响的待办、通知与上报：
  - 通知按 `(recall, vehicle, scope_version)` 去重，历史版本保留可查。
  - 待办按版本重建：已确认维修置 `done`，否则 `open` 并分配给车辆所在国网点；上一版本仍 `open` 的置 `cancelled`（保留历史）。
  - 上报按范围版本生成；车辆转手/登记时原地重算当前版本上报的受影响集合。
- **历史可查**：已确认维修、历史通知、历史上报和重算记录均保留，不被删除。

## 并发与原子性

- 服务层用可重入锁串行化写操作；维修提交在事务内原子完成「扣减备件 + 建立维修单」，并通过条件更新 `available>0` 与行数校验，库存不足则整笔回滚——绝不允许扣了库存却没有维修单。
- 最后一箱备件同一时间只能被一台车用掉；失败后保留原提交，按同一幂等键（`idempotency_key`）重试，返回确定性结果且不重复建单。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

当前为本地原型：跨境规则用许可字符串模拟，零件库存与维修记录是简化模型，不包含真实 VIN 解码、监管接口、物流系统或法定通知渠道。
