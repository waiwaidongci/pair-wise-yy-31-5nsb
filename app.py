#!/usr/bin/env python3
"""Vehicle safety recall publication, transfer, remedy and completion tracker."""
from __future__ import annotations

import argparse
import hashlib
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
        super().__init__(message); self.status, self.message = status, message


class Store:
    def __init__(self, path: str | Path = DB_PATH):
        self.path = str(path)
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
        CREATE TABLE IF NOT EXISTS notifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          vehicle_id INTEGER NOT NULL REFERENCES vehicles(id), scope_version INTEGER NOT NULL,
          channel TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
          UNIQUE(recall_id,vehicle_id,scope_version)
        );
        CREATE TABLE IF NOT EXISTS regulatory_reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          scope_version INTEGER NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(recall_id,scope_version)
        );
        CREATE TABLE IF NOT EXISTS todos (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          vehicle_id INTEGER NOT NULL REFERENCES vehicles(id), dealer_id INTEGER REFERENCES dealers(id),
          scope_version INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN ('open','done','cancelled')),
          reason TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(recall_id,vehicle_id,scope_version)
        );
        CREATE INDEX IF NOT EXISTS idx_todos_dealer ON todos(dealer_id,status);
        CREATE TABLE IF NOT EXISTS recalculations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, recall_id INTEGER NOT NULL REFERENCES recalls(id),
          scope_version INTEGER NOT NULL, trigger TEXT NOT NULL, vehicle_id INTEGER REFERENCES vehicles(id),
          reason TEXT NOT NULL, affected_count INTEGER NOT NULL DEFAULT 0,
          created_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        """)
        # 兼容旧库：为 regulatory_reports 补 updated_at 列
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info(regulatory_reports)")]
        if "updated_at" not in cols:
            self.conn.execute("ALTER TABLE regulatory_reports ADD COLUMN updated_at TEXT")
            self.conn.execute("UPDATE regulatory_reports SET updated_at=created_at WHERE updated_at IS NULL")
        self.conn.commit()

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def close(self) -> None:
        self.conn.close()


def locked(method):
    """串行化服务调用，保证并发维修提交时库存扣减与维修单创建的原子性。"""
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


class RecallService:
    def __init__(self, store: Store):
        self.store, self.conn = store, store.conn
        self._lock = threading.RLock()

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role not in allowed: raise ApiError(403, "角色无权执行此操作")
        return actor

    def _row(self, table: str, identity: object, column: str = "id") -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE {column}=?", (identity,)).fetchone()
        if not row: raise ApiError(404, "对象不存在")
        return row

    @locked
    def register_dealer(self, actor: str | None, role: str | None, code: str, name: str, country: str) -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if not code or not country: raise ApiError(400, "维修网点代号和国家不能为空")
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO dealers(code,name,country) VALUES(?,?,?)", (code, name, country))
                self.store.audit(actor, "dealer.register", "dealer", cur.lastrowid, {"code": code, "country": country})
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "维修网点代号已存在") from exc
        return {"id": cur.lastrowid, "code": code, "name": name, "country": country, "active": True}

    @locked
    def register_vehicle(self, actor: str | None, role: str | None, vin: str, model: str, model_year: int, country: str, owner_name: str) -> dict:
        actor = self._actor(actor, role, {"manufacturer", "regulator"})
        vin = vin.upper().strip()
        if len(vin) < 5 or not model or int(model_year) < 1900: raise ApiError(400, "车辆识别信息不完整")
        try:
            with self.conn:
                cur = self.conn.execute("INSERT INTO vehicles(vin,model,model_year,country,origin_country,owner_name,updated_at) VALUES(?,?,?,?,?,?,?)",
                                        (vin, model, int(model_year), country, country, owner_name, now()))
                self.store.audit(actor, "vehicle.register", "vehicle", cur.lastrowid, {"vin": vin, "country": country})
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "车辆识别码已存在") from exc
        # 新车登记即纳入当前已发布召回的范围核对
        for recall in self.conn.execute("SELECT * FROM recalls WHERE state='published' ORDER BY id"):
            self._recalculate(recall["id"], recall["scope_version"], "vehicle_registered", actor,
                              vehicle_id=cur.lastrowid, reason=f"车辆登记：{vin}")
        return {"id": cur.lastrowid, "vin": vin, "model": model, "model_year": model_year, "country": country, "owner_name": owner_name}

    @locked
    def transfer_vehicle(self, actor: str | None, role: str | None, vin: str, country: str, owner_name: str) -> dict:
        actor = self._actor(actor, role, {"dealer", "regulator"})
        vehicle = self._row("vehicles", vin.upper(), "vin")
        old_country = vehicle["country"]
        with self.conn:
            self.conn.execute("UPDATE vehicles SET country=?,owner_name=?,updated_at=? WHERE id=?", (country, owner_name, now(), vehicle["id"]))
            self.store.audit(actor, "vehicle.transfer", "vehicle", vehicle["id"], {"old_country": old_country, "new_country": country, "owner_name": owner_name})
        # 所在国一变，重算受影响召回的待办、通知与上报
        for recall in self.conn.execute("SELECT * FROM recalls WHERE state='published' ORDER BY id"):
            self._recalculate(recall["id"], recall["scope_version"], "vehicle_transferred", actor,
                              vehicle_id=vehicle["id"], reason=f"车辆跨境转手：{old_country} → {country}")
        return dict(self._row("vehicles", vehicle["id"]))

    @locked
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

    @locked
    def submit_recall(self, actor: str | None, role: str | None, recall_id: int, expected_version: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer"})
        recall = self._row("recalls", recall_id)
        if recall["created_by"] != actor: raise ApiError(403, "只能提交本机构创建的召回")
        if recall["state"] != "draft": raise ApiError(409, "只有草稿可以提交")
        return self._recall_state_change(recall, "submitted", expected_version, actor, "提交监管审核")

    @locked
    def review_recall(self, actor: str | None, role: str | None, recall_id: int, decision: str, expected_version: int, note: str = "") -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if decision not in {"publish", "return"}: raise ApiError(400, "决定只能是 publish 或 return")
        recall = self._row("recalls", recall_id)
        if recall["state"] != "submitted": raise ApiError(409, "只有已提交召回可以审核")
        if int(expected_version) != int(recall["revision"]): raise ApiError(409, "召回已被修改，请刷新版本")
        state = "published" if decision == "publish" else "returned"
        result = self._recall_state_change(recall, state, expected_version, actor, note)
        if state == "published":
            self._create_release_artifacts(recall["id"], int(recall["scope_version"]), actor)
            result = self._recall_dict(self._row("recalls", recall_id))
        return result

    def _recall_state_change(self, recall: sqlite3.Row, state: str, expected_version: int, actor: str, note: str) -> dict:
        if int(expected_version) != int(recall["revision"]): raise ApiError(409, "召回版本冲突")
        with self.conn:
            cur = self.conn.execute("UPDATE recalls SET state=?,revision=revision+1,review_note=?,updated_at=? WHERE id=? AND revision=?",
                                    (state, note, now(), recall["id"], expected_version))
            if cur.rowcount != 1: raise ApiError(409, "并发更新冲突")
            self.store.audit(actor, f"recall.{state}", "recall", recall["id"], {"note": note, "scope_version": recall["scope_version"]})
        return self._recall_dict(self._row("recalls", recall["id"]))

    @locked
    def change_scope(self, actor: str | None, role: str | None, recall_id: int, scope: dict, expected_version: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer"})
        self._validate_scope(scope)
        recall = self._row("recalls", recall_id)
        if recall["manufacturer"] != actor: raise ApiError(403, "只能调整本机构的召回范围")
        if recall["state"] != "published": raise ApiError(409, "只有已发布召回可以调整范围")
        if int(expected_version) != int(recall["revision"]): raise ApiError(409, "召回已被修改，请刷新版本")
        scope_version = int(recall["scope_version"]) + 1
        with self.conn:
            self.conn.execute("UPDATE recalls SET scope_json=?,scope_version=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                              (j(scope), scope_version, now(), recall_id, expected_version))
            self.conn.execute("INSERT INTO scope_changes(recall_id,scope_version,scope_json,created_by,created_at) VALUES(?,?,?,?,?)",
                              (recall_id, scope_version, j(scope), actor, now()))
            self.store.audit(actor, "recall.scope_change", "recall", recall_id, {"scope_version": scope_version, "scope": scope})
        self._create_release_artifacts(recall_id, scope_version, actor)
        return self._recall_dict(self._row("recalls", recall_id))

    @locked
    def add_parts(self, actor: str | None, role: str | None, recall_id: int, dealer_id: int, remedy_version: int, quantity: int) -> dict:
        actor = self._actor(actor, role, {"manufacturer", "regulator"})
        if quantity <= 0: raise ApiError(400, "入库数量必须大于零")
        recall = self._row("recalls", recall_id); dealer = self._row("dealers", dealer_id)
        if recall["state"] not in {"published", "submitted"}: raise ApiError(409, "召回尚未进入可备件状态")
        with self.conn:
            self.conn.execute("""INSERT INTO parts(recall_id,dealer_id,remedy_version,available) VALUES(?,?,?,?)
                               ON CONFLICT(recall_id,dealer_id,remedy_version) DO UPDATE SET available=available+excluded.available""",
                              (recall_id, dealer_id, remedy_version, quantity))
            self.store.audit(actor, "parts.add", "recall", recall_id, {"dealer_id": dealer_id, "quantity": quantity, "remedy_version": remedy_version})
        row = self.conn.execute("SELECT * FROM parts WHERE recall_id=? AND dealer_id=? AND remedy_version=?", (recall_id, dealer_id, remedy_version)).fetchone()
        return dict(row)

    @locked
    def report_repair(self, actor: str | None, role: str | None, recall_id: int, vin: str, dealer_id: int, remedy_version: int, evidence_hash: str, evidence_consistent: bool, border_permit: str = "", idempotency_key: str = "") -> dict:
        actor = self._actor(actor, role, {"dealer"})
        if not idempotency_key or not evidence_hash: raise ApiError(400, "证据哈希和幂等键不能为空")
        recall = self._row("recalls", recall_id)
        if recall["state"] != "published": raise ApiError(409, "召回尚未发布")
        if int(remedy_version) != int(recall["remedy_version"]): raise ApiError(409, "维修方案版本不是当前版本")
        vehicle = self._row("vehicles", vin.upper(), "vin")
        dealer = self._row("dealers", dealer_id)
        if not dealer["active"]: raise ApiError(409, "维修网点已停用")
        existing = self.conn.execute("SELECT * FROM repairs WHERE recall_id=? AND vehicle_id=? AND idempotency_key=?", (recall_id, vehicle["id"], idempotency_key)).fetchone()
        if existing: return dict(existing)
        duplicate = self.conn.execute("SELECT id FROM repairs WHERE recall_id=? AND vehicle_id=? AND status IN ('reported','confirmed')", (recall_id, vehicle["id"])).fetchone()
        if duplicate: raise ApiError(409, "该车辆已有维修记录")
        scope = json.loads(recall["scope_json"])
        if not self._in_scope(vehicle, scope): raise ApiError(409, "车辆不在当前召回范围内")
        cross_border = dealer["country"] != vehicle["country"]
        if cross_border and not border_permit.strip(): raise ApiError(403, "跨境维修需要有效许可")
        part = self.conn.execute("SELECT * FROM parts WHERE recall_id=? AND dealer_id=? AND remedy_version=?", (recall_id, dealer_id, remedy_version)).fetchone()
        if not part or int(part["available"]) < 1: raise ApiError(409, "维修网点零件库存不足")
        # 原子扣减：条件更新 + 行数校验，库存不足则整笔回滚，绝不允许扣了库存却没有维修单
        with self.conn:
            cur = self.conn.execute("""INSERT INTO repairs(recall_id,vehicle_id,dealer_id,remedy_version,status,evidence_hash,evidence_consistent,
                                     cross_border,border_permit,idempotency_key,reported_by,reported_at)
                                     VALUES(?,?,?,?, 'reported',?,?,?,?,?,?,?)""",
                                    (recall_id, vehicle["id"], dealer_id, remedy_version, evidence_hash, int(evidence_consistent), int(cross_border), border_permit, idempotency_key, actor, now()))
            repair_id = cur.lastrowid
            cur = self.conn.execute("UPDATE parts SET available=available-1 WHERE id=? AND available>0", (part["id"],))
            if cur.rowcount != 1:
                raise ApiError(409, "零件已被其他维修占用或库存不足，请按同一流水号重试")
            self.store.audit(actor, "repair.report", "repair", repair_id, {"recall_id": recall_id, "vin": vehicle["vin"], "cross_border": cross_border})
        return dict(self._row("repairs", repair_id))

    @locked
    def review_repair(self, actor: str | None, role: str | None, repair_id: int, decision: str, note: str = "") -> dict:
        actor = self._actor(actor, role, {"regulator"})
        if decision not in {"confirm", "flag"}: raise ApiError(400, "决定只能是 confirm 或 flag")
        repair = self._row("repairs", repair_id)
        if repair["status"] != "reported": raise ApiError(409, "维修记录已经复核")
        new_status = "confirmed" if decision == "confirm" and repair["evidence_consistent"] else "flagged"
        with self.conn:
            self.conn.execute("UPDATE repairs SET status=?,reviewed_by=?,reviewed_at=?,review_note=? WHERE id=?", (new_status, actor, now(), note, repair_id))
            if new_status == "flagged":
                self.conn.execute("UPDATE parts SET available=available+1 WHERE recall_id=? AND dealer_id=? AND remedy_version=?",
                                  (repair["recall_id"], repair["dealer_id"], repair["remedy_version"]))
            else:
                # 维修确认：把该车辆当前召回范围内的待办置为已完成
                self.conn.execute("""UPDATE todos SET status='done', reason='repair_confirmed', updated_at=?
                                     WHERE recall_id=? AND vehicle_id=? AND status='open'""",
                                  (now(), repair["recall_id"], repair["vehicle_id"]))
            self.store.audit(actor, "repair.review", "repair", repair_id, {"decision": decision, "status": new_status, "note": note})
        return dict(self._row("repairs", repair_id))

    def _create_release_artifacts(self, recall_id: int, scope_version: int, actor: str) -> None:
        trigger = "recall_published" if int(scope_version) == 1 else "scope_changed"
        self._recalculate(recall_id, scope_version, trigger, actor, reason=("召回发布" if trigger == "recall_published" else "召回范围调整"))

    def _recalculate(self, recall_id: int, scope_version: int, trigger: str, actor: str,
                     vehicle_id: int | None = None, reason: str | None = None) -> None:
        """范围或所在国变化后，重算受影响的待办、通知与上报。

        - 通知：为当前范围内的车辆补建当前范围版本的通知（历史版本保留不动）。
        - 待办：按当前范围重建该版本待办；已确认维修的置为 done，否则 open 并分配给车辆所在国网点；
                上一版本仍 open 的待办置为 cancelled（保留历史）。
        - 上报：范围变化时生成新版本监管上报；车辆转手时原地重算当前版本上报的受影响集合。
        """
        recall = self._row("recalls", recall_id)
        scope = json.loads(recall["scope_json"])
        vehicles = [row for row in self.conn.execute("SELECT * FROM vehicles ORDER BY id")]
        in_scope = [v for v in vehicles if self._in_scope(v, scope)]
        stamp = now()
        with self.conn:
            for v in in_scope:
                self.conn.execute("""INSERT OR IGNORE INTO notifications(recall_id,vehicle_id,scope_version,channel,status,created_at)
                                     VALUES(?,?,?, 'owner-notice','queued',?)""", (recall_id, v["id"], scope_version, stamp))
                confirmed = self.conn.execute("SELECT dealer_id FROM repairs WHERE recall_id=? AND vehicle_id=? AND status='confirmed'",
                                              (recall_id, v["id"])).fetchone()
                if confirmed:
                    t_status, t_dealer = "done", confirmed["dealer_id"]
                else:
                    t_status = "open"
                    dealer = self.conn.execute("SELECT id FROM dealers WHERE country=? AND active=1 ORDER BY id LIMIT 1",
                                               (v["country"],)).fetchone()
                    t_dealer = dealer["id"] if dealer else None
                self.conn.execute("""INSERT INTO todos(recall_id,vehicle_id,dealer_id,scope_version,status,reason,created_at,updated_at)
                                     VALUES(?,?,?,?,?,?,?,?)
                                     ON CONFLICT(recall_id,vehicle_id,scope_version) DO UPDATE SET
                                       dealer_id=excluded.dealer_id, status=excluded.status,
                                       reason=excluded.reason, updated_at=excluded.updated_at""",
                                  (recall_id, v["id"], t_dealer, scope_version, t_status, reason or trigger, stamp, stamp))
            if trigger == "scope_changed":
                self.conn.execute("UPDATE todos SET status='cancelled', reason=?, updated_at=? WHERE recall_id=? AND scope_version=? AND status='open'",
                                  (reason or trigger, stamp, recall_id, int(scope_version) - 1))
            payload = {"campaign_code": recall["campaign_code"], "scope_version": scope_version, "scope": scope,
                      "remedy_version": recall["remedy_version"], "affected_count": len(in_scope),
                      "vins": [v["vin"] for v in in_scope]}
            if trigger in ("vehicle_registered", "vehicle_transferred"):
                self.conn.execute("UPDATE regulatory_reports SET payload_json=?, updated_at=? WHERE recall_id=? AND scope_version=?",
                                  (j(payload), stamp, recall_id, scope_version))
            else:
                self.conn.execute("""INSERT OR IGNORE INTO regulatory_reports(recall_id,scope_version,payload_json,status,created_at,updated_at)
                                     VALUES(?,?,?, 'queued',?,?)""", (recall_id, scope_version, j(payload), stamp, stamp))
            self.conn.execute("""INSERT INTO recalculations(recall_id,scope_version,trigger,vehicle_id,reason,affected_count,created_by,created_at)
                                 VALUES(?,?,?,?,?,?,?,?)""",
                              (recall_id, scope_version, trigger, vehicle_id, reason or trigger, len(in_scope), actor, stamp))
            self.store.audit(actor, f"recall.recalculate.{trigger}", "recall", recall_id,
                            {"scope_version": scope_version, "vehicle_id": vehicle_id, "affected_count": len(in_scope)})

    @locked
    def unfinished(self, actor: str | None, role: str | None, recall_id: int) -> dict:
        self._actor(actor, role, {"manufacturer", "regulator"})
        recall = self._row("recalls", recall_id)
        scope = json.loads(recall["scope_json"])
        confirmed = {row["vehicle_id"] for row in self.conn.execute("SELECT vehicle_id FROM repairs WHERE recall_id=? AND status='confirmed'", (recall_id,))}
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
            if not scope.get(key): raise ApiError(400, f"召回范围缺少 {key}")

    def _recall_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "manufacturer": row["manufacturer"], "campaign_code": row["campaign_code"], "title": row["title"],
                "scope": json.loads(row["scope_json"]), "scope_version": row["scope_version"], "remedy": json.loads(row["remedy_json"]),
                "remedy_version": row["remedy_version"], "state": row["state"], "revision": row["revision"], "review_note": row["review_note"]}

    @locked
    def recall_detail(self, recall_id: int) -> dict:
        result = self._recall_dict(self._row("recalls", recall_id))
        result["repairs"] = [dict(row) for row in self.conn.execute("SELECT * FROM repairs WHERE recall_id=? ORDER BY id", (recall_id,))]
        result["reports"] = [dict(row) for row in self.conn.execute("SELECT * FROM regulatory_reports WHERE recall_id=? ORDER BY scope_version", (recall_id,))]
        result["notifications"] = [dict(row) for row in self.conn.execute("SELECT * FROM notifications WHERE recall_id=? ORDER BY id", (recall_id,))]
        result["todos"] = [dict(row) for row in self.conn.execute("SELECT * FROM todos WHERE recall_id=? ORDER BY id", (recall_id,))]
        result["recalculations"] = [dict(row) for row in self.conn.execute("SELECT * FROM recalculations WHERE recall_id=? ORDER BY id", (recall_id,))]
        return result

    @locked
    def reconciliation(self, recall_id: int) -> dict:
        """把召回范围、车辆流转、网点库存、维修记录和监管上报接成一份对账结果。"""
        recall = self._recall_dict(self._row("recalls", recall_id))
        scope = recall["scope"]
        vehicles = [dict(row) for row in self.conn.execute("SELECT * FROM vehicles ORDER BY vin")]
        dealers = [dict(row) for row in self.conn.execute("SELECT * FROM dealers ORDER BY id")]
        repairs = [dict(row) for row in self.conn.execute("SELECT * FROM repairs WHERE recall_id=? ORDER BY id", (recall_id,))]
        notifications = [dict(row) for row in self.conn.execute("SELECT * FROM notifications WHERE recall_id=? ORDER BY id", (recall_id,))]
        todos = [dict(row) for row in self.conn.execute("SELECT * FROM todos WHERE recall_id=? ORDER BY id", (recall_id,))]
        parts = [dict(row) for row in self.conn.execute("SELECT * FROM parts WHERE recall_id=? ORDER BY id", (recall_id,))]
        reports = [dict(row) for row in self.conn.execute("SELECT * FROM regulatory_reports WHERE recall_id=? ORDER BY scope_version", (recall_id,))]
        recalculations = [dict(row) for row in self.conn.execute("SELECT * FROM recalculations WHERE recall_id=? ORDER BY id", (recall_id,))]
        scope_versions = [dict(row) for row in self.conn.execute("SELECT * FROM scope_changes WHERE recall_id=? ORDER BY scope_version", (recall_id,))]

        in_scope = {v["id"]: self._in_scope(self.conn.execute("SELECT * FROM vehicles WHERE id=?", (v["id"],)).fetchone(), scope) for v in vehicles}
        repair_by_vehicle: dict[int, dict] = {}
        for r in repairs:
            repair_by_vehicle.setdefault(r["vehicle_id"], r)  # 最新一条
        notif_versions: dict[int, list[int]] = {}
        for n in notifications:
            notif_versions.setdefault(n["vehicle_id"], []).append(n["scope_version"])
        todo_by_vehicle: dict[int, dict] = {}
        for t in todos:
            todo_by_vehicle[t["vehicle_id"]] = t  # 最大 scope_version 覆盖

        part_key = {(p["dealer_id"], p["remedy_version"]): p for p in parts}
        vehicle_views = []
        for v in vehicles:
            vid = v["id"]
            repair = repair_by_vehicle.get(vid)
            todo = todo_by_vehicle.get(vid)
            notified = sorted(notif_versions.get(vid, []))
            shortage = False
            if in_scope[vid] and (not repair or repair["status"] != "confirmed") and todo and todo["status"] == "open":
                p = part_key.get((todo["dealer_id"], recall["remedy_version"])) if todo["dealer_id"] else None
                shortage = (not p) or p["available"] < 1
            vehicle_views.append({
                "vin": v["vin"], "model": v["model"], "model_year": v["model_year"],
                "country": v["country"], "origin_country": v["origin_country"], "owner_name": v["owner_name"],
                "in_scope": in_scope[vid],
                "current_scope_version": max(notified) if notified else None,
                "notified_versions": notified,
                "repair_status": repair["status"] if repair else None,
                "repair_id": repair["id"] if repair else None,
                "todo_status": todo["status"] if todo else None,
                "todo_dealer_id": todo["dealer_id"] if todo else None,
                "shortage": shortage,
            })

        dealer_views = []
        for d in dealers:
            d_parts = [p for p in parts if p["dealer_id"] == d["id"]]
            open_todos = [t for t in todos if t["dealer_id"] == d["id"] and t["status"] == "open"]
            shortage_vins = []
            for t in open_todos:
                v = next((x for x in vehicles if x["id"] == t["vehicle_id"]), None)
                p = next((p for p in d_parts if p["remedy_version"] == recall["remedy_version"]), None)
                if not p or p["available"] < 1:
                    shortage_vins.append(v["vin"] if v else t["vehicle_id"])
            dealer_views.append({
                "dealer_id": d["id"], "code": d["code"], "name": d["name"], "country": d["country"], "active": d["active"],
                "parts": d_parts,
                "open_todos": len(open_todos),
                "shortage_count": len(shortage_vins),
                "shortage_vins": shortage_vins,
                "current_scope_version": recall["scope_version"],
            })

        return {
            "recall": recall,
            "scope_versions": scope_versions,
            "vehicles": vehicle_views,
            "dealers": dealer_views,
            "notifications": notifications,
            "todos": todos,
            "regulatory_reports": reports,
            "recalculations": recalculations,
        }

    @locked
    def state(self) -> dict:
        return {"dealers": [dict(row) for row in self.conn.execute("SELECT * FROM dealers ORDER BY id")],
                "vehicles": [dict(row) for row in self.conn.execute("SELECT * FROM vehicles ORDER BY id")],
                "recalls": [self._recall_dict(row) for row in self.conn.execute("SELECT * FROM recalls ORDER BY id DESC")],
                "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    @locked
    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM dealers LIMIT 1").fetchone():
            self.register_dealer("regulator-demo", "regulator", "D-CN", "演示中心", "CN")
        if not self.conn.execute("SELECT id FROM recalls LIMIT 1").fetchone():
            recall = self.create_recall("maker-demo", "manufacturer", "RC-2026-001", "制动管路检查", {"models": ["X1"], "model_years": [2018, 2019], "vin_prefixes": ["LX"], "countries": ["CN"]}, {"version": 1, "description": "更换制动管"})
            self.submit_recall("maker-demo", "manufacturer", recall["id"], recall["revision"])


class Handler(BaseHTTPRequestHandler):
    service: RecallService

    def log_message(self, fmt: str, *args: object) -> None: sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode(); self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try: return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc: raise ApiError(400, "JSON 请求体无效") from exc

    def _parts(self) -> list[str]: return [p for p in urlparse(self.path).path.strip("/").split("/") if p]

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]): out = {"status": "ok"}
            elif p == ["api", "state"]: out = self.service.state()
            elif len(p) == 3 and p[:2] == ["api", "recalls"]: out = self.service.recall_detail(int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "unfinished":
                out = self.service.unfinished(self.headers.get("X-Actor"), self.headers.get("X-Role"), int(p[2]))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "reconciliation":
                out = self.service.reconciliation(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(page))); self.end_headers(); self.wfile.write(page); return
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except Exception as exc: self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, body = self._parts(), self._body(); actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role")
            if p == ["api", "dealers"]: out = self.service.register_dealer(actor, role, body.get("code", ""), body.get("name", ""), body.get("country", ""))
            elif p == ["api", "vehicles"]: out = self.service.register_vehicle(actor, role, body.get("vin", ""), body.get("model", ""), int(body.get("model_year", 0)), body.get("country", ""), body.get("owner_name", ""))
            elif len(p) == 4 and p[:2] == ["api", "vehicles"] and p[3] == "transfer": out = self.service.transfer_vehicle(actor, role, p[2], body.get("country", ""), body.get("owner_name", ""))
            elif p == ["api", "recalls"]: out = self.service.create_recall(actor, role, body.get("campaign_code", ""), body.get("title", ""), body.get("scope", {}), body.get("remedy", {}))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "submit": out = self.service.submit_recall(actor, role, int(p[2]), int(body.get("expected_version", -1)))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "review": out = self.service.review_recall(actor, role, int(p[2]), body.get("decision", ""), int(body.get("expected_version", -1)), body.get("note", ""))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "scope": out = self.service.change_scope(actor, role, int(p[2]), body.get("scope", {}), int(body.get("expected_version", -1)))
            elif len(p) == 4 and p[:2] == ["api", "recalls"] and p[3] == "parts": out = self.service.add_parts(actor, role, int(p[2]), int(body.get("dealer_id", 0)), int(body.get("remedy_version", 0)), int(body.get("quantity", 0)))
            elif p == ["api", "repairs"]: out = self.service.report_repair(actor, role, int(body.get("recall_id", 0)), body.get("vin", ""), int(body.get("dealer_id", 0)), int(body.get("remedy_version", 0)), body.get("evidence_hash", ""), bool(body.get("evidence_consistent", True)), body.get("border_permit", ""), body.get("idempotency_key", ""))
            elif len(p) == 4 and p[:2] == ["api", "repairs"] and p[3] == "review": out = self.service.review_repair(actor, role, int(p[2]), body.get("decision", ""), body.get("note", ""))
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send(exc.status, {"error": exc.message})
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc: self._send(400, {"error": str(exc)})
        except Exception as exc: self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path); service = RecallService(store)
    if seed: service.seed()
    Handler.service = service
    print(f"vehicle recall listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--port", type=int, default=8213); parser.add_argument("--db", default=str(DB_PATH)); parser.add_argument("--init", action="store_true"); parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init: Store(args.db).close()
    if args.seed or not args.init: run(args.port, args.db, args.seed)


if __name__ == "__main__": main()
