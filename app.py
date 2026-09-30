#!/usr/bin/env python3
"""Vehicle safety recall publication, transfer, remedy and completion tracker.

召回范围、车辆流转、网点库存、维修记录、监管上报五本账通过统一的对账视图衔接：
- 范围版本发布 / 调整，或车辆登记 / 跨境转手后，按当前 scope_version 重算
  范围归属(scope_memberships)、车主通知(notifications) 与监管上报(regulatory_reports)，
  重算过程留痕(recompute_runs)，历史通知与已确认维修原样保留。
- 维修提交在同一事务内"条件扣减备件成功后才落维修单"，配合写锁串行化，
  最后一箱备件只会被一台车用掉；库存不足时提交内容保留在 repair_attempts，
  车方可凭原流水号(idempotency_key)重试，绝不出现扣了库存却没有维修单。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = Path(__file__).with_name("data.db")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def j(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


class _PartsShortage(Exception):
    """内部信号：备件条件扣减未命中一行，事务回滚后保留提交。"""


class Store:
    def __init__(self, path: str | Path = DB_PATH):
        self.path = str(path)
        # 跨线程共享同一连接：所有"先查后写"的业务流程必须持 write_lock 串行化
        self.write_lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS dealers (
          id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
          country TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS vehicles (
          id INTEGER PRIMARY KEY AUTOINCREMENT, vin TEXT UNIQUE NOT NULL, model TEXT NOT NULL,
          model_year INTEGER NOT NULL, country TEXT NOT NULL, origin_country TEXT NOT NULL, owner_name TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS recalls (
          id INTEGER PRIMARY KEY AUTOINCREMENT, manufacturer TEXT NOT NULL, campaign_code TEXT UNIQUE NOT NULL,
          title TEXT NOT NULL, scope_json TEXT NOT NULL, remedy_version INTEGER NOT NULL,
          remedy_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('draft','submitted','published','returned')),
          scope_version INTEGER NOT NULL DEFAULT 1, revision INTEGER NOT NULL DEFAULT 1,
          review_note TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scope_changes (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          scope_version INTEGER NOT NULL, scope_json TEXT NOT NULL, created_by TEXT NOT NULL,
          created_at TEXT NOT NULL, UNIQUE(recall_id,scope_version)
        );
        CREATE TABLE IF NOT EXISTS parts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          dealer_id INTEGER NOT NULL REFERENCES dealers(id), remedy_version INTEGER NOT NULL,
          available INTEGER NOT NULL CHECK(available>=0), UNIQUE(recall_id,dealer_id,remedy_version)
        );
        CREATE TABLE IF NOT EXISTS part_ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          dealer_id INTEGER NOT NULL REFERENCES dealers(id), remedy_version INTEGER NOT NULL,
          delta INTEGER NOT NULL, reason TEXT NOT NULL, repair_id INTEGER, actor TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS repairs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          vehicle_id INTEGER NOT NULL REFERENCES vehicles(id), dealer_id INTEGER NOT NULL REFERENCES dealers(id),
          remedy_version INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN ('reported','confirmed','flagged')),
          evidence_hash TEXT NOT NULL, evidence_consistent INTEGER NOT NULL, cross_border INTEGER NOT NULL DEFAULT 0,
          border_permit TEXT, idempotency_key TEXT NOT NULL, reported_by TEXT NOT NULL,
          reported_at TEXT NOT NULL, reviewed_by TEXT, reviewed_at TEXT, review_note TEXT,
          UNIQUE(recall_id,vehicle_id,idempotency_key)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_confirmed_repair ON repairs(recall_id,vehicle_id) WHERE status='confirmed';
        CREATE TABLE IF NOT EXISTS repair_attempts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          vehicle_id INTEGER NOT NULL REFERENCES vehicles(id), dealer_id INTEGER NOT NULL REFERENCES dealers(id),
          remedy_version INTEGER NOT NULL, evidence_hash TEXT NOT NULL, evidence_consistent INTEGER NOT NULL,
          border_permit TEXT, idempotency_key TEXT NOT NULL, submitted_by TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('failed','accepted')), fail_reason TEXT,
          attempts INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(recall_id,vehicle_id,idempotency_key)
        );
        CREATE TABLE IF NOT EXISTS scope_memberships (
          recall_id INTEGER NOT NULL REFERENCES recalls(id), vehicle_id INTEGER NOT NULL REFERENCES vehicles(id),
          scope_version INTEGER NOT NULL, in_scope INTEGER NOT NULL, last_reason TEXT NOT NULL,
          updated_at TEXT NOT NULL, PRIMARY KEY(recall_id,vehicle_id)
        );
        CREATE TABLE IF NOT EXISTS notifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          vehicle_id INTEGER NOT NULL REFERENCES vehicles(id), scope_version INTEGER NOT NULL,
          channel TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
          UNIQUE(recall_id,vehicle_id,scope_version)
        );
        CREATE TABLE IF NOT EXISTS regulatory_reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          scope_version INTEGER NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
          created_at TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '',
          UNIQUE(recall_id,scope_version)
        );
        CREATE TABLE IF NOT EXISTS recompute_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          scope_version INTEGER NOT NULL, trigger TEXT NOT NULL, reason TEXT NOT NULL,
          affected_count INTEGER NOT NULL, actor TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        """)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        def cols(table: str) -> set[str]:
            return {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}

        if "regulatory_reports" in {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            present = cols("regulatory_reports")
            if "reason" not in present:
                self.conn.execute("ALTER TABLE regulatory_reports ADD COLUMN reason TEXT NOT NULL DEFAULT ''")
            if "updated_at" not in present:
                self.conn.execute("ALTER TABLE regulatory_reports ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
            self.conn.execute("UPDATE regulatory_reports SET updated_at=created_at WHERE updated_at=''")
            self.conn.execute("UPDATE regulatory_reports SET reason='初始生成' WHERE reason=''")

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def close(self) -> None:
        self.conn.close()


class RecallService:
    def __init__(self, store: Store):
        self.store, self.conn = store, store.conn

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor:
            raise ApiError(401, "缺少身份")
        if role not in allowed:
            raise ApiError(403, "角色无权执行此操作")
        return actor

    def _row(self, table: str, identity: object, column: str = "id") -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE {column}=?", (identity,)).fetchone()
        if not row:
            raise ApiError(404, "对象不存在")
        return row

    # ---- 网点 / 车辆 -------------------------------------------------------

    def register_dealer(self, actor: str | None, role: str | None, code: str, name: str, country: str) -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if not code or not country:
            raise ApiError(400, "维修网点代号和国家不能为空")
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO dealers(code,name,country) VALUES(?,?,?)", (code, name, country))
                self.store.audit(actor, "dealer.register", "dealer", cur.lastrowid, {"code": code, "country": country})
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "维修网点代号已存在") from exc
        return {"id": cur.lastrowid, "code": code, "name": name, "country": country, "active": True}

    def register_vehicle(self, actor: str | None, role: str | None, vin: str, model: str, model_year: int, country: str, owner_name: str) -> dict:
        actor = self._actor(actor, role, {"manufacturer", "regulator"})
        vin = vin.upper().strip()
        if len(vin) < 5 or not model or int(model_year) < 1900:
            raise ApiError(400, "车辆识别信息不完整")
        stamp = now()
        try:
            with self.conn:
                cur = self.conn.execute("""INSERT INTO vehicles(vin,model,model_year,country,origin_country,owner_name,updated_at)
                                         VALUES(?,?,?,?,?,?,?)""",
                                        (vin, model, int(model_year), country, country, owner_name, stamp))
                self.store.audit(actor, "vehicle.register", "vehicle", cur.lastrowid, {"vin": vin, "country": country})
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "车辆识别码已存在") from exc
        vehicle_id = cur.lastrowid
        # 新登记车辆同样要立刻落入已发布召回的当前版本，不能等下一次改范围才被看见
        for recall_id in self._published_recall_ids():
            self._recompute(recall_id, "vehicle_register", actor,
                            f"车辆登记：{vin} 落籍 {country}，按当前版本重算范围归属、待办与上报",
                            only_vehicle_id=vehicle_id)
        return {"id": vehicle_id, "vin": vin, "model": model, "model_year": model_year, "country": country, "owner_name": owner_name}

    def transfer_vehicle(self, actor: str | None, role: str | None, vin: str, country: str, owner_name: str) -> dict:
        actor = self._actor(actor, role, {"dealer", "regulator"})
        vin = vin.upper()
        vehicle = self._row("vehicles", vin, "vin")
        old_country = vehicle["country"]
        if not country:
            raise ApiError(400, "转入国家不能为空")
        with self.conn:
            self.conn.execute("UPDATE vehicles SET country=?,owner_name=?,updated_at=? WHERE id=?",
                              (country, owner_name, now(), vehicle["id"]))
            self.store.audit(actor, "vehicle.transfer", "vehicle", vehicle["id"],
                             {"old_country": old_country, "new_country": country, "owner_name": owner_name})
        # 所在国一变，所有已发布召回都要按当前范围版本重算，跨境转手才不会错过最新范围
        for recall_id in self._published_recall_ids():
            self._recompute(recall_id, "vehicle_transfer", actor,
                            f"跨境转手：{vin} 所在国 {old_country}→{country}，按当前版本重算范围归属、待办、通知与上报",
                            only_vehicle_id=vehicle["id"])
        return dict(self._row("vehicles", vehicle["id"]))

    # ---- 召回发布与范围版本 ----------------------------------------------

    def create_recall(self, actor: str | None, role: str | None, campaign_code: str, title: str, scope: dict, remedy: dict) -> dict:
        actor = self._actor(actor, role, {"manufacturer"})
        self._validate_scope(scope)
        if not campaign_code.strip() or not remedy.get("description") or not remedy.get("version"):
            raise ApiError(400, "召回活动编号和修复方案不能为空")
        stamp = now()
        try:
            with self.conn:
                cur = self.conn.execute("""INSERT INTO recalls(manufacturer,campaign_code,title,scope_json,remedy_version,remedy_json,state,created_by,created_at,updated_at)
                                         VALUES(?,?,?,?,?,?, 'draft',?,?,?)""",
                                        (actor, campaign_code, title, j(scope), int(remedy["version"]), j(remedy), actor, stamp, stamp))
                self.store.audit(actor, "recall.create", "recall", cur.lastrowid, {"campaign_code": campaign_code, "remedy_version": remedy["version"]})
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "召回活动编号已存在") from exc
        return self._recall_dict(self._row("recalls", cur.lastrowid))

    def submit_recall(self, actor: str | None, role: str | None, recall_id: int, expected_version: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer"})
        recall = self._row("recalls", recall_id)
        if recall["created_by"] != actor:
            raise ApiError(403, "只能提交本机构创建的召回")
        if recall["state"] != "draft":
            raise ApiError(409, "只有草稿可以提交")
        return self._recall_state_change(recall, "submitted", expected_version, actor, "提交监管审核")

    def review_recall(self, actor: str | None, role: str | None, recall_id: int, decision: str, expected_version: int, note: str = "") -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if decision not in {"publish", "return"}:
            raise ApiError(400, "决定只能是 publish 或 return")
        recall = self._row("recalls", recall_id)
        if recall["state"] != "submitted":
            raise ApiError(409, "只有已提交召回可以审核")
        if int(expected_version) != int(recall["revision"]):
            raise ApiError(409, "召回已被修改，请刷新版本")
        state = "published" if decision == "publish" else "returned"
        result = self._recall_state_change(recall, state, expected_version, actor, note)
        if state == "published":
            self._recompute(recall_id, "publish", actor,
                            f"监管发布：按初始范围 v{recall['scope_version']} 重算受影响待办、通知与上报")
            result = self._recall_dict(self._row("recalls", recall_id))
        return result

    def _recall_state_change(self, recall: sqlite3.Row, state: str, expected_version: int, actor: str, note: str) -> dict:
        if int(expected_version) != int(recall["revision"]):
            raise ApiError(409, "召回版本冲突")
        with self.conn:
            cur = self.conn.execute("UPDATE recalls SET state=?,revision=revision+1,review_note=?,updated_at=? WHERE id=? AND revision=?",
                                    (state, note, now(), recall["id"], expected_version))
            if cur.rowcount != 1:
                raise ApiError(409, "并发更新冲突")
            self.store.audit(actor, f"recall.{state}", "recall", recall["id"], {"note": note, "scope_version": recall["scope_version"]})
        return self._recall_dict(self._row("recalls", recall["id"]))

    def change_scope(self, actor: str | None, role: str | None, recall_id: int, scope: dict, expected_version: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer"})
        self._validate_scope(scope)
        recall = self._row("recalls", recall_id)
        if recall["manufacturer"] != actor:
            raise ApiError(403, "只能调整本机构的召回范围")
        if recall["state"] != "published":
            raise ApiError(409, "只有已发布召回可以调整范围")
        if int(expected_version) != int(recall["revision"]):
            raise ApiError(409, "召回已被修改，请刷新版本")
        scope_version = int(recall["scope_version"]) + 1
        with self.conn:
            self.conn.execute("UPDATE recalls SET scope_json=?,scope_version=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                              (j(scope), scope_version, now(), recall_id, expected_version))
            self.conn.execute("INSERT INTO scope_changes(recall_id,scope_version,scope_json,created_by,created_at) VALUES(?,?,?,?,?)",
                              (recall_id, scope_version, j(scope), actor, now()))
            self.store.audit(actor, "recall.scope_change", "recall", recall_id, {"scope_version": scope_version, "scope": scope})
        # 范围一变：受影响待办、通知、上报全部按新版本重算（历史通知与已确认维修保留）
        self._recompute(recall_id, "scope_change", actor,
                        f"范围调整生效：按 v{scope_version} 重算受影响待办、通知与上报")
        return self._recall_dict(self._row("recalls", recall_id))

    # ---- 库存与维修（原子扣减 + 流水号重试） ------------------------------

    def add_parts(self, actor: str | None, role: str | None, recall_id: int, dealer_id: int, remedy_version: int, quantity: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer", "regulator"})
        if quantity <= 0:
            raise ApiError(400, "入库数量必须大于零")
        recall = self._row("recalls", recall_id)
        dealer = self._row("dealers", dealer_id)
        if recall["state"] not in {"published", "submitted"}:
            raise ApiError(409, "召回尚未进入可备件状态")
        stamp = now()
        with self.store.write_lock, self.conn:
            self.conn.execute("""INSERT INTO parts(recall_id,dealer_id,remedy_version,available) VALUES(?,?,?,?)
                               ON CONFLICT(recall_id,dealer_id,remedy_version) DO UPDATE SET available=available+excluded.available""",
                              (recall_id, dealer_id, remedy_version, quantity))
            self.conn.execute("""INSERT INTO part_ledger(recall_id,dealer_id,remedy_version,delta,reason,repair_id,actor,created_at)
                                 VALUES(?,?,?,?,'inbound',NULL,?,?)""",
                              (recall_id, dealer_id, remedy_version, quantity, actor, stamp))
            self.store.audit(actor, "parts.add", "recall", recall_id,
                             {"dealer_id": dealer_id, "quantity": quantity, "remedy_version": remedy_version})
        row = self.conn.execute("SELECT * FROM parts WHERE recall_id=? AND dealer_id=? AND remedy_version=?",
                                (recall_id, dealer_id, remedy_version)).fetchone()
        return dict(row)

    def report_repair(self, actor: str | None, role: str | None, recall_id: int, vin: str, dealer_id: int,
                      remedy_version: int, evidence_hash: str, evidence_consistent: bool,
                      border_permit: str = "", idempotency_key: str = "") -> dict:
        actor = self._actor(actor, role, {"dealer"})
        if not idempotency_key or not evidence_hash:
            raise ApiError(400, "证据哈希和幂等键不能为空")
        recall = self._row("recalls", recall_id)
        if recall["state"] != "published":
            raise ApiError(409, "召回尚未发布")
        if int(remedy_version) != int(recall["remedy_version"]):
            raise ApiError(409, "维修方案版本不是当前版本")
        vehicle = self._row("vehicles", vin.upper(), "vin")
        dealer = self._row("dealers", dealer_id)
        if not dealer["active"]:
            raise ApiError(409, "维修网点已停用")
        cross_border = dealer["country"] != vehicle["country"]
        if cross_border and not border_permit.strip():
            raise ApiError(403, "跨境维修需要有效许可")
        scope = json.loads(recall["scope_json"])
        if not self._in_scope(vehicle, scope):
            raise ApiError(409, "车辆不在当前召回范围内")

        with self.store.write_lock:
            # 锁内复查，避免两笔并发提交互相穿透"先查后写"
            existing = self.conn.execute(
                "SELECT * FROM repairs WHERE recall_id=? AND vehicle_id=? AND idempotency_key=?",
                (recall_id, vehicle["id"], idempotency_key)).fetchone()
            if existing:
                return dict(existing)  # 同一流水号重放：直接回单，不再扣库存
            duplicate = self.conn.execute(
                "SELECT id FROM repairs WHERE recall_id=? AND vehicle_id=? AND status IN ('reported','confirmed')",
                (recall_id, vehicle["id"])).fetchone()
            if duplicate:
                raise ApiError(409, "该车辆已有维修记录")

            stamp = now()
            try:
                with self.conn:
                    part = self.conn.execute(
                        "SELECT * FROM parts WHERE recall_id=? AND dealer_id=? AND remedy_version=?",
                        (recall_id, dealer_id, remedy_version)).fetchone()
                    if part is None:
                        raise _PartsShortage()
                    # 先做条件扣减：只有确实扣到一箱，才允许写维修单
                    consumed = self.conn.execute(
                        "UPDATE parts SET available=available-1 WHERE id=? AND available>=1", (part["id"],))
                    if consumed.rowcount != 1:
                        raise _PartsShortage()
                    cur = self.conn.execute("""INSERT INTO repairs(recall_id,vehicle_id,dealer_id,remedy_version,status,evidence_hash,
                                             evidence_consistent,cross_border,border_permit,idempotency_key,reported_by,reported_at)
                                             VALUES(?,?,?,?, 'reported',?,?,?,?,?,?,?)""",
                                            (recall_id, vehicle["id"], dealer_id, remedy_version, evidence_hash,
                                             int(evidence_consistent), int(cross_border), border_permit,
                                             idempotency_key, actor, stamp))
                    self.conn.execute("""INSERT INTO part_ledger(recall_id,dealer_id,remedy_version,delta,reason,repair_id,actor,created_at)
                                         VALUES(?,?,?,-1,'repair_consume',?,?,?)""",
                                      (recall_id, dealer_id, remedy_version, cur.lastrowid, actor, stamp))
                    self.conn.execute("""INSERT INTO repair_attempts(recall_id,vehicle_id,dealer_id,remedy_version,evidence_hash,
                                             evidence_consistent,border_permit,idempotency_key,submitted_by,status,attempts,created_at,updated_at)
                                             VALUES(?,?,?,?,?,?,?,?,?, 'accepted',1,?,?)
                                         ON CONFLICT(recall_id,vehicle_id,idempotency_key) DO UPDATE SET
                                             dealer_id=excluded.dealer_id, evidence_hash=excluded.evidence_hash,
                                             evidence_consistent=excluded.evidence_consistent, border_permit=excluded.border_permit,
                                             status='accepted', fail_reason=NULL, attempts=attempts+1, updated_at=excluded.updated_at""",
                                      (recall_id, vehicle["id"], dealer_id, remedy_version, evidence_hash, int(evidence_consistent),
                                       border_permit, idempotency_key, actor, stamp, stamp))
                    self.store.audit(actor, "repair.report", "repair", cur.lastrowid,
                                     {"recall_id": recall_id, "vin": vehicle["vin"], "cross_border": cross_border,
                                      "idempotency_key": idempotency_key})
            except _PartsShortage:
                # 上面的事务整体回滚：库存没动、维修单没落。本次提交原样保留，按同一流水号可重试
                self._retain_failed_submission(recall_id, vehicle, dealer_id, remedy_version, evidence_hash,
                                               evidence_consistent, border_permit, idempotency_key, actor, stamp)
                raise ApiError(409, f"网点该备件库存不足，提交已保留（流水号 {idempotency_key}），备件到货后可用同一流水号重试")
        return dict(self._row("repairs", cur.lastrowid))

    def _retain_failed_submission(self, recall_id: int, vehicle: sqlite3.Row, dealer_id: int, remedy_version: int,
                                  evidence_hash: str, evidence_consistent: bool, border_permit: str,
                                  idempotency_key: str, actor: str, stamp: str) -> None:
        with self.conn:
            self.conn.execute("""INSERT INTO repair_attempts(recall_id,vehicle_id,dealer_id,remedy_version,evidence_hash,
                                     evidence_consistent,border_permit,idempotency_key,submitted_by,status,fail_reason,attempts,created_at,updated_at)
                                     VALUES(?,?,?,?,?,?,?,?,?, 'failed','parts_shortage',1,?,?)
                                 ON CONFLICT(recall_id,vehicle_id,idempotency_key) DO UPDATE SET
                                     dealer_id=excluded.dealer_id, evidence_hash=excluded.evidence_hash,
                                     evidence_consistent=excluded.evidence_consistent, border_permit=excluded.border_permit,
                                     status='failed', fail_reason='parts_shortage', attempts=attempts+1, updated_at=excluded.updated_at""",
                              (recall_id, vehicle["id"], dealer_id, remedy_version, evidence_hash, int(evidence_consistent),
                               border_permit, idempotency_key, actor, stamp, stamp))
            self.store.audit(actor, "repair.shortage_retained", "repair_attempt", idempotency_key,
                             {"recall_id": recall_id, "vin": vehicle["vin"], "dealer_id": dealer_id})

    def review_repair(self, actor: str | None, role: str | None, repair_id: int, decision: str, note: str = "") -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if decision not in {"confirm", "flag"}:
            raise ApiError(400, "决定只能是 confirm 或 flag")
        repair = self._row("repairs", repair_id)
        if repair["status"] != "reported":
            raise ApiError(409, "维修记录已经复核")
        new_status = "confirmed" if decision == "confirm" and repair["evidence_consistent"] else "flagged"
        stamp = now()
        with self.store.write_lock, self.conn:
            self.conn.execute("UPDATE repairs SET status=?,reviewed_by=?,reviewed_at=?,review_note=? WHERE id=?",
                              (new_status, actor, stamp, note, repair_id))
            if new_status == "flagged":
                self.conn.execute("UPDATE parts SET available=available+1 WHERE recall_id=? AND dealer_id=? AND remedy_version=?",
                                  (repair["recall_id"], repair["dealer_id"], repair["remedy_version"]))
                self.conn.execute("""INSERT INTO part_ledger(recall_id,dealer_id,remedy_version,delta,reason,repair_id,actor,created_at)
                                     VALUES(?,?,?,1,'flagged_return',?,?,?)""",
                                  (repair["recall_id"], repair["dealer_id"], repair["remedy_version"], repair_id, actor, stamp))
            self.store.audit(actor, "repair.review", "repair", repair_id, {"decision": decision, "status": new_status, "note": note})
        return dict(self._row("repairs", repair_id))

    # ---- 范围重算 ---------------------------------------------------------

    def _published_recall_ids(self) -> list[int]:
        return [r["id"] for r in self.conn.execute("SELECT id FROM recalls WHERE state='published' ORDER BY id")]

    def _recompute(self, recall_id: int, trigger: str, actor: str, reason: str, only_vehicle_id: int | None = None) -> dict | None:
        """按当前 scope_version 重算范围归属、通知和监管上报。

        只对当前在范围内的车辆补当前版本通知（INSERT OR IGNORE，历史版本不动）；
        监管上报按 (recall, scope_version) 原地刷新为最新受影响清单并重新排队；
        已确认维修不受范围变化影响，继续可查。
        """
        with self.store.write_lock:
            recall = self._row("recalls", recall_id)
            if recall["state"] != "published":
                return None
            scope_version = int(recall["scope_version"])
            scope = json.loads(recall["scope_json"])
            vehicles = list(self.conn.execute("SELECT * FROM vehicles ORDER BY id"))
            affected = [v for v in vehicles if self._in_scope(v, scope)]
            stamp = now()
            with self.conn:
                targets = vehicles if only_vehicle_id is None else [v for v in vehicles if v["id"] == only_vehicle_id]
                for vehicle in targets:
                    self.conn.execute("""INSERT INTO scope_memberships(recall_id,vehicle_id,scope_version,in_scope,last_reason,updated_at)
                                         VALUES(?,?,?,?,?,?)
                                         ON CONFLICT(recall_id,vehicle_id) DO UPDATE SET
                                             scope_version=excluded.scope_version, in_scope=excluded.in_scope,
                                             last_reason=excluded.last_reason, updated_at=excluded.updated_at""",
                                      (recall_id, vehicle["id"], scope_version, int(self._in_scope(vehicle, scope)), reason, stamp))
                for vehicle in affected:
                    self.conn.execute("""INSERT OR IGNORE INTO notifications(recall_id,vehicle_id,scope_version,channel,status,created_at)
                                         VALUES(?,?,?, 'owner-notice','queued',?)""",
                                      (recall_id, vehicle["id"], scope_version, stamp))
                payload = {"campaign_code": recall["campaign_code"], "scope_version": scope_version, "scope": scope,
                           "remedy_version": recall["remedy_version"], "affected_count": len(affected),
                           "affected_vins": [v["vin"] for v in affected], "recompute_trigger": trigger}
                self.conn.execute("""INSERT INTO regulatory_reports(recall_id,scope_version,payload_json,status,created_at,reason,updated_at)
                                     VALUES(?,?,?, 'queued',?,?,?)
                                     ON CONFLICT(recall_id,scope_version) DO UPDATE SET
                                         payload_json=excluded.payload_json, status='queued',
                                         reason=excluded.reason, updated_at=excluded.updated_at""",
                                  (recall_id, scope_version, j(payload), stamp, reason, stamp))
                self.conn.execute("""INSERT INTO recompute_runs(recall_id,scope_version,trigger,reason,affected_count,actor,created_at)
                                     VALUES(?,?,?,?,?,?,?)""",
                                  (recall_id, scope_version, trigger, reason, len(affected), actor, stamp))
                self.store.audit(actor, "recall.recompute", "recall", recall_id,
                                 {"scope_version": scope_version, "trigger": trigger, "affected_count": len(affected),
                                  "only_vehicle_id": only_vehicle_id})
            return {"scope_version": scope_version, "affected_count": len(affected), "trigger": trigger, "reason": reason}

    # ---- 对账结果 ---------------------------------------------------------

    def reconciliation(self, recall_id: int | None = None) -> dict:
        if recall_id is not None:
            return self._reconcile_one(self._row("recalls", recall_id))
        rows = self.conn.execute("SELECT * FROM recalls WHERE state='published' ORDER BY id").fetchall()
        return {"generated_at": now(), "recalls": [self._reconcile_one(r) for r in rows]}

    def _reconcile_one(self, recall: sqlite3.Row) -> dict:
        recall_id = recall["id"]
        scope_version = int(recall["scope_version"])
        vehicles = list(self.conn.execute("SELECT * FROM vehicles ORDER BY vin"))
        memberships = {r["vehicle_id"]: r for r in self.conn.execute(
            "SELECT * FROM scope_memberships WHERE recall_id=?", (recall_id,))}
        repairs = list(self.conn.execute("""SELECT rp.*, d.code AS dealer_code, d.name AS dealer_name, v.vin AS vin
                                            FROM repairs rp JOIN dealers d ON d.id=rp.dealer_id JOIN vehicles v ON v.id=rp.vehicle_id
                                            WHERE rp.recall_id=? ORDER BY rp.id""", (recall_id,)))
        repairs_by_vehicle: dict[int, list[sqlite3.Row]] = {}
        for r in repairs:
            repairs_by_vehicle.setdefault(r["vehicle_id"], []).append(r)
        confirmed = {r["vehicle_id"] for r in repairs if r["status"] == "confirmed"}
        notifs = list(self.conn.execute(
            "SELECT * FROM notifications WHERE recall_id=? ORDER BY scope_version, id", (recall_id,)))
        notifs_by_vehicle: dict[int, list[sqlite3.Row]] = {}
        for n in notifs:
            notifs_by_vehicle.setdefault(n["vehicle_id"], []).append(n)
        attempts = list(self.conn.execute("""SELECT a.*, v.vin AS vin FROM repair_attempts a JOIN vehicles v ON v.id=a.vehicle_id
                                             WHERE a.recall_id=? ORDER BY a.id""", (recall_id,)))
        open_backorders = [a for a in attempts if a["status"] == "failed"]
        parts_rows = list(self.conn.execute("""SELECT p.*, d.code AS dealer_code, d.name AS dealer_name, d.country AS dealer_country
                                               FROM parts p JOIN dealers d ON d.id=p.dealer_id
                                               WHERE p.recall_id=? ORDER BY d.code, p.remedy_version""", (recall_id,)))
        ledger = {(r["dealer_id"], r["remedy_version"]): r["total"] for r in self.conn.execute(
            "SELECT dealer_id, remedy_version, SUM(delta) AS total FROM part_ledger WHERE recall_id=? GROUP BY dealer_id, remedy_version",
            (recall_id,))}
        runs = list(self.conn.execute("SELECT * FROM recompute_runs WHERE recall_id=? ORDER BY id DESC", (recall_id,)))
        last_run = runs[0] if runs else None
        reports = [self._report_dict(r) for r in self.conn.execute(
            "SELECT * FROM regulatory_reports WHERE recall_id=? ORDER BY scope_version", (recall_id,))]

        vehicle_rows = []
        for v in vehicles:
            m = memberships.get(v["id"])
            v_repairs = repairs_by_vehicle.get(v["id"], [])
            current_repair = next((r for r in v_repairs if r["status"] == "confirmed"), v_repairs[-1] if v_repairs else None)
            backorder = next((a for a in open_backorders if a["vehicle_id"] == v["id"]), None)
            in_scope = bool(m and m["in_scope"])
            vehicle_rows.append({
                "vin": v["vin"], "model": v["model"], "model_year": v["model_year"],
                "country": v["country"], "origin_country": v["origin_country"], "owner_name": v["owner_name"],
                "in_scope": in_scope,
                "scope_version": m["scope_version"] if m else None,
                "recompute_reason": m["last_reason"] if m else None,
                "membership_updated_at": m["updated_at"] if m else None,
                "todo": in_scope and v["id"] not in confirmed,
                "repair": None if current_repair is None else {
                    "id": current_repair["id"], "status": current_repair["status"],
                    "remedy_version": current_repair["remedy_version"], "dealer_code": current_repair["dealer_code"],
                    "dealer_name": current_repair["dealer_name"], "cross_border": bool(current_repair["cross_border"]),
                    "reported_at": current_repair["reported_at"], "review_note": current_repair["review_note"]},
                "repair_history": [{"id": r["id"], "status": r["status"], "remedy_version": r["remedy_version"],
                                    "dealer_code": r["dealer_code"], "reported_at": r["reported_at"]} for r in v_repairs],
                "backorder": None if backorder is None else {
                    "idempotency_key": backorder["idempotency_key"], "dealer_id": backorder["dealer_id"],
                    "attempts": backorder["attempts"], "reason": backorder["fail_reason"], "updated_at": backorder["updated_at"]},
                "notifications": [{"scope_version": n["scope_version"], "channel": n["channel"],
                                   "status": n["status"], "created_at": n["created_at"]} for n in notifs_by_vehicle.get(v["id"], [])],
            })

        affected_count = sum(1 for row in vehicle_rows if row["in_scope"])
        todo_count = sum(1 for row in vehicle_rows if row["todo"])
        backorders_by_dealer: dict[tuple[int, int], list[sqlite3.Row]] = {}
        for a in open_backorders:
            backorders_by_dealer.setdefault((a["dealer_id"], a["remedy_version"]), []).append(a)
        dealer_rows = []
        for p in parts_rows:
            demand = sum(1 for v in vehicles
                         if (m := memberships.get(v["id"])) and m["in_scope"] and v["country"] == p["dealer_country"]
                         and v["id"] not in confirmed)
            bo = backorders_by_dealer.get((p["dealer_id"], p["remedy_version"]), [])
            dealer_rows.append({
                "dealer_id": p["dealer_id"], "dealer_code": p["dealer_code"], "dealer_name": p["dealer_name"],
                "country": p["dealer_country"], "remedy_version": p["remedy_version"],
                "available": p["available"], "demand_in_country": demand,
                "projected_gap": max(0, demand - p["available"]),
                "backorder_count": len(bo),
                "backorders": [{"vin": a["vin"], "idempotency_key": a["idempotency_key"],
                                "attempts": a["attempts"], "updated_at": a["updated_at"]} for a in bo],
                "ledger_total": ledger.get((p["dealer_id"], p["remedy_version"]), 0),
                "recompute_reason": last_run["reason"] if last_run else None,
            })

        checks = self._reconcile_checks(recall_id, scope_version, parts_rows, ledger, repairs,
                                        vehicle_rows, reports, notifs, attempts)
        return {
            "recall_id": recall_id, "campaign_code": recall["campaign_code"], "title": recall["title"],
            "state": recall["state"], "scope_version": scope_version, "remedy_version": recall["remedy_version"],
            "affected_count": affected_count, "todo_count": todo_count,
            "confirmed_count": len(confirmed), "backorder_count": len(open_backorders),
            "last_recompute": None if last_run is None else {
                "trigger": last_run["trigger"], "reason": last_run["reason"],
                "scope_version": last_run["scope_version"], "affected_count": last_run["affected_count"],
                "actor": last_run["actor"], "created_at": last_run["created_at"]},
            "recompute_runs": [{"scope_version": r["scope_version"], "trigger": r["trigger"], "reason": r["reason"],
                                "affected_count": r["affected_count"], "actor": r["actor"], "created_at": r["created_at"]}
                               for r in runs[:20]],
            "vehicles": vehicle_rows, "dealers": dealer_rows, "reports": reports, "checks": checks,
        }

    @staticmethod
    def _report_dict(row: sqlite3.Row) -> dict:
        payload = json.loads(row["payload_json"])
        return {"scope_version": row["scope_version"], "status": row["status"], "reason": row["reason"],
                "created_at": row["created_at"], "updated_at": row["updated_at"], "payload": payload}

    def _reconcile_checks(self, recall_id: int, scope_version: int, parts_rows: list[sqlite3.Row],
                          ledger: dict, repairs: list[sqlite3.Row], vehicle_rows: list[dict],
                          reports: list[dict], notifs: list[sqlite3.Row], attempts: list[sqlite3.Row]) -> list[dict]:
        checks = []
        bad = [{"dealer_id": p["dealer_id"], "remedy_version": p["remedy_version"],
                "available": p["available"], "ledger_total": ledger.get((p["dealer_id"], p["remedy_version"]), 0)}
               for p in parts_rows if p["available"] != ledger.get((p["dealer_id"], p["remedy_version"]), 0)]
        checks.append({"name": "库存账实一致（台账结余=网点库存）", "ok": not bad, "detail": bad or "全部备件批次台账结余与库存一致"})

        consumed_ledger = sum(1 for r in self.conn.execute(
            "SELECT id FROM part_ledger WHERE recall_id=? AND reason='repair_consume'", (recall_id,)))
        active_repairs = sum(1 for r in repairs if r["status"] in ("reported", "confirmed"))
        checks.append({"name": "每笔消耗都有维修单（不出现扣了库存没有维修单）",
                       "ok": consumed_ledger == active_repairs,
                       "detail": {"consumed_ledger": consumed_ledger, "active_repairs": active_repairs}})

        missing_notice = [row["vin"] for row in vehicle_rows
                          if row["in_scope"] and not any(n["scope_version"] == scope_version for n in row["notifications"])]
        checks.append({"name": "当前范围内车辆通知无遗漏", "ok": not missing_notice,
                       "detail": missing_notice or f"v{scope_version} 全部受影响车辆均已生成通知"})

        versions = sorted(r["scope_version"] for r in reports)
        checks.append({"name": "监管上报覆盖全部范围版本", "ok": versions == list(range(1, scope_version + 1)),
                       "detail": {"report_versions": versions, "expected": list(range(1, scope_version + 1))}})

        latest = reports[-1] if reports else None
        current_affected = sum(1 for row in vehicle_rows if row["in_scope"])
        checks.append({"name": "最新上报受影响数量已按当前版本重算",
                       "ok": bool(latest and latest["payload"]["affected_count"] == current_affected),
                       "detail": {"report_count": latest["payload"]["affected_count"] if latest else None,
                                  "current_count": current_affected}})

        retained = [row["vin"] for row in vehicle_rows if row["repair"] and not row["in_scope"]]
        history_versions = sorted({n["scope_version"] for n in notifs})
        checks.append({"name": "历史可追溯（出范围车辆的已确认维修与历史通知保留）", "ok": True,
                       "detail": {"confirmed_repair_vin_out_of_scope": retained, "notification_versions": history_versions}})

        failed_open = [{"vin": a["vin"], "idempotency_key": a["idempotency_key"], "attempts": a["attempts"]}
                       for a in attempts if a["status"] == "failed"]
        checks.append({"name": "欠件提交可凭原流水号重试", "ok": True,
                       "detail": failed_open or "无挂起欠件提交"})
        return checks

    def unfinished(self, actor: str | None, role: str | None, recall_id: int) -> dict:
        self._actor(actor, role, {"manufacturer", "regulator"})
        recall = self._row("recalls", recall_id)
        scope = json.loads(recall["scope_json"])
        confirmed = {row["vehicle_id"] for row in self.conn.execute(
            "SELECT vehicle_id FROM repairs WHERE recall_id=? AND status='confirmed'", (recall_id,))}
        current_year = datetime.now(timezone.utc).year
        items = []
        for vehicle in self.conn.execute("SELECT * FROM vehicles ORDER BY vin"):
            if self._in_scope(vehicle, scope) and vehicle["id"] not in confirmed:
                items.append({"vin": vehicle["vin"], "model": vehicle["model"], "model_year": vehicle["model_year"],
                              "country": vehicle["country"], "risk": "high" if current_year - int(vehicle["model_year"]) >= 8 else "normal"})
        return {"recall_id": recall_id, "scope_version": recall["scope_version"], "unfinished_count": len(items), "vehicles": items}

    @staticmethod
    def _in_scope(vehicle: sqlite3.Row, scope: dict) -> bool:
        return (vehicle["model"] in scope.get("models", []) and int(vehicle["model_year"]) in scope.get("model_years", [])
                and any(vehicle["vin"].startswith(prefix.upper()) for prefix in scope.get("vin_prefixes", []))
                and (vehicle["country"] in scope.get("countries", []) or vehicle["origin_country"] in scope.get("countries", [])))

    @staticmethod
    def _validate_scope(scope: dict) -> None:
        for key in ("models", "model_years", "vin_prefixes", "countries"):
            if not scope.get(key):
                raise ApiError(400, f"召回范围缺少 {key}")

    def _recall_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "manufacturer": row["manufacturer"], "campaign_code": row["campaign_code"], "title": row["title"],
                "scope": json.loads(row["scope_json"]), "scope_version": row["scope_version"], "remedy": json.loads(row["remedy_json"]),
                "remedy_version": row["remedy_version"], "state": row["state"], "revision": row["revision"], "review_note": row["review_note"]}

    def recall_detail(self, recall_id: int) -> dict:
        result = self._recall_dict(self._row("recalls", recall_id))
        result["repairs"] = [dict(row) for row in self.conn.execute("SELECT * FROM repairs WHERE recall_id=? ORDER BY id", (recall_id,))]
        result["reports"] = [self._report_dict(row) for row in self.conn.execute(
            "SELECT * FROM regulatory_reports WHERE recall_id=? ORDER BY scope_version", (recall_id,))]
        return result

    def state(self) -> dict:
        return {"dealers": [dict(row) for row in self.conn.execute("SELECT * FROM dealers ORDER BY id")],
                "vehicles": [dict(row) for row in self.conn.execute("SELECT * FROM vehicles ORDER BY id")],
                "recalls": [self._recall_dict(row) for row in self.conn.execute("SELECT * FROM recalls ORDER BY id DESC")],
                "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM dealers LIMIT 1").fetchone():
            self.register_dealer("regulator-demo", "regulator", "D-CN", "中国中心", "CN")
            self.register_dealer("regulator-demo", "regulator", "D-SG", "新加坡中心", "SG")
        if not self.conn.execute("SELECT id FROM recalls LIMIT 1").fetchone():
            scope = {"models": ["X1"], "model_years": [2018, 2019], "vin_prefixes": ["LX"], "countries": ["CN"]}
            recall = self.create_recall("maker-demo", "manufacturer", "RC-2026-001", "制动管路检查",
                                        scope, {"version": 1, "description": "更换制动管"})
            recall = self.submit_recall("maker-demo", "manufacturer", recall["id"], recall["revision"])
            self.review_recall("regulator-demo", "regulator", recall["id"], "publish", recall["revision"], "同意发布")
            self.register_vehicle("maker-demo", "manufacturer", "LX20260001", "X1", 2018, "CN", "张三")
            self.register_vehicle("maker-demo", "manufacturer", "LX20260002", "X1", 2019, "CN", "李四")
            self.add_parts("maker-demo", "manufacturer", recall["id"], 1, 1, 1)
            self.add_parts("maker-demo", "manufacturer", recall["id"], 2, 1, 2)
            self.transfer_vehicle("dealer", "dealer", "LX20260002", "SG", "Tan")


class Handler(BaseHTTPRequestHandler):
    service: RecallService

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try:
            return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc:
            raise ApiError(400, "JSON 请求体无效") from exc

    def _parts(self) -> list[str]:
        return [p for p in urlparse(self.path).path.strip("/").split("/") if p]

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]):
                out = {"status": "ok"}
            elif p == ["api", "state"]:
                out = self.service.state()
            elif p == ["api", "reconciliation"]:
                out = self.service.reconciliation(None)
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "reconciliation":
                out = self.service.reconciliation(int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "unfinished":
                out = self.service.unfinished(self.headers.get("X-Actor"), self.headers.get("X-Role"), int(p[2]))
            elif len(p) == 3 and p[:2] == ["api", "recalls"]:
                out = self.service.recall_detail(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)
                return
            else:
                raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc:
            self._send(exc.status, {"error": exc.message})
        except Exception as exc:
            self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, body = self._parts(), self._body()
            actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role")
            if p == ["api", "dealers"]:
                out = self.service.register_dealer(actor, role, body.get("code", ""), body.get("name", ""), body.get("country", ""))
            elif p == ["api", "vehicles"]:
                out = self.service.register_vehicle(actor, role, body.get("vin", ""), body.get("model", ""),
                                                    int(body.get("model_year", 0)), body.get("country", ""), body.get("owner_name", ""))
            elif len(p) == 4 and p[:2] == ["api", "vehicles"] and p[3] == "transfer":
                out = self.service.transfer_vehicle(actor, role, p[2], body.get("country", ""), body.get("owner_name", ""))
            elif p == ["api", "recalls"]:
                out = self.service.create_recall(actor, role, body.get("campaign_code", ""), body.get("title", ""),
                                                 body.get("scope", {}), body.get("remedy", {}))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "submit":
                out = self.service.submit_recall(actor, role, int(p[2]), int(body.get("expected_version", -1)))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "review":
                out = self.service.review_recall(actor, role, int(p[2]), body.get("decision", ""),
                                                 int(body.get("expected_version", -1)), body.get("note", ""))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "scope":
                out = self.service.change_scope(actor, role, int(p[2]), body.get("scope", {}), int(body.get("expected_version", -1)))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "parts":
                out = self.service.add_parts(actor, role, int(p[2]), int(body.get("dealer_id", 0)),
                                             int(body.get("remedy_version", 0)), int(body.get("quantity", 0)))
            elif p == ["api", "repairs"]:
                out = self.service.report_repair(actor, role, int(body.get("recall_id", 0)), body.get("vin", ""),
                                                 int(body.get("dealer_id", 0)), int(body.get("remedy_version", 0)),
                                                 body.get("evidence_hash", ""), bool(body.get("evidence_consistent", True)),
                                                 body.get("border_permit", ""), body.get("idempotency_key", ""))
            elif len(p) == 4 and p[:2] == ["api", "repairs"] and p[3] == "review":
                out = self.service.review_repair(actor, role, int(p[2]), body.get("decision", ""), body.get("note", ""))
            else:
                raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc:
            self._send(exc.status, {"error": exc.message})
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc:
            self._send(400, {"error": str(exc)})
        except Exception as exc:
            self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path)
    service = RecallService(store)
    if seed:
        service.seed()
    Handler.service = service
    print(f"vehicle recall listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8213)
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init:
        Store(args.db).close()
    if args.seed or not args.init:
        run(args.port, args.db, args.seed)


if __name__ == "__main__":
    main()
