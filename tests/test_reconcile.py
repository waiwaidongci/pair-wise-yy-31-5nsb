import threading
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, RecallService, Store


class ReconcileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = RecallService(Store(Path(self.tmp.name) / "r.db"))
        self.d_cn = self.s.register_dealer("reg", "regulator", "D-CN", "中国中心", "CN")
        self.d_sg = self.s.register_dealer("reg", "regulator", "D-SG", "新加坡中心", "SG")

    def tearDown(self):
        self.s.store.close()
        self.tmp.cleanup()

    def make_recall(self, code="RC-1", countries=("CN",)):
        scope = {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": list(countries)}
        r = self.s.create_recall("maker", "manufacturer", code, "制动检查", scope, {"version": 1, "description": "更换软管"})
        r = self.s.submit_recall("maker", "manufacturer", r["id"], r["revision"])
        return self.s.review_recall("reg", "regulator", r["id"], "publish", r["revision"], "同意发布")

    def test_cross_border_transfer_recomputes_latest_scope(self):
        """车辆跨境转手落入最新范围：补当前版本通知、刷新上报，但历史通知与已确认维修保留。"""
        recall = self.make_recall()
        v = self.s.register_vehicle("maker", "manufacturer", "LX00001", "X", 2018, "SG", "Tan")
        # SG 不在范围内：无 v1 通知、待办为零
        self.assertEqual(0, self.s.unfinished("reg", "regulator", recall["id"])["unfinished_count"])

        self.s.transfer_vehicle("dealer", "dealer", "LX00001", "CN", "谭")
        recon = self.s.reconciliation(recall["id"])
        self.assertEqual(1, recon["affected_count"])
        self.assertEqual(1, recon["todo_count"])
        veh = recon["vehicles"][0]
        self.assertTrue(veh["in_scope"])
        self.assertEqual(1, veh["scope_version"])
        self.assertIn("跨境转手", veh["recompute_reason"])
        self.assertEqual(1, len(veh["notifications"]))  # 转入后补发 v1 通知
        self.assertEqual(1, recon["reports"][0]["payload"]["affected_count"])  # 上报同步重算
        self.assertEqual("queued", recon["reports"][0]["status"])
        self.assertTrue(all(c["ok"] for c in recon["checks"]))

        # 出范围：确认维修与历史通知继续可查
        self.s.add_parts("maker", "manufacturer", recall["id"], self.d_cn["id"], 1, 1)
        repair = self.s.report_repair("dealer", "dealer", recall["id"], "LX00001", self.d_cn["id"],
                                      1, "h1", True, idempotency_key="k1")
        self.s.review_repair("reg", "regulator", repair["id"], "confirm", "ok")
        changed = self.s.change_scope("maker", "manufacturer", recall["id"],
                                      {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["JP"]},
                                      recall["revision"])
        self.assertEqual(2, changed["scope_version"])
        recon = self.s.reconciliation(recall["id"])
        veh = recon["vehicles"][0]
        self.assertFalse(veh["in_scope"])
        self.assertEqual("confirmed", veh["repair"]["status"])      # 已确认维修继续可查
        self.assertEqual([1], [n["scope_version"] for n in veh["notifications"]])  # 历史通知保留，不重复
        self.assertEqual([1, 2], [r["scope_version"] for r in recon["reports"]])
        self.assertEqual(0, recon["reports"][1]["payload"]["affected_count"])

    def test_concurrent_repairs_last_box_single_winner_and_retry(self):
        """两笔维修并发：最后一箱备件只扣一次；失败提交保留，原流水号重试成功。"""
        recall = self.make_recall(countries=("CN", "SG"))
        self.s.register_vehicle("maker", "manufacturer", "LX00010", "X", 2018, "CN", "甲")
        self.s.register_vehicle("maker", "manufacturer", "LX00011", "X", 2018, "CN", "乙")
        self.s.add_parts("maker", "manufacturer", recall["id"], self.d_cn["id"], 1, 1)
        results = {}

        def submit(key, vin, order):
            barrier.wait()
            try:
                r = self.s.report_repair("dealer", "dealer", recall["id"], vin, self.d_cn["id"],
                                         1, f"h-{key}", True, idempotency_key=key)
                results[key] = ("ok", r["id"])
            except ApiError as exc:
                results[key] = ("err", exc.status, exc.message)

        barrier = threading.Barrier(2)
        t1 = threading.Thread(target=submit, args=("k-a", "LX00010", 0))
        t2 = threading.Thread(target=submit, args=("k-b", "LX00011", 1))
        t1.start(); t2.start(); t1.join(); t2.join()

        statuses = {k: v[0] for k, v in results.items()}
        self.assertEqual(1, sum(1 for x in statuses.values() if x == "ok"))
        failed_key = next(k for k, v in results.items() if v[0] == "err")
        self.assertEqual(409, results[failed_key][1])
        self.assertIn("流水号", results[failed_key][2])

        # 库存确实只剩 0，且只有一张维修单
        self.assertEqual(0, self.s.conn.execute("SELECT available FROM parts").fetchone()["available"])
        self.assertEqual(1, self.s.conn.execute("SELECT COUNT(*) c FROM repairs").fetchone()["c"])

        # 不能扣了库存却没有维修单：消耗台账条数 = 有效维修单数
        recon = self.s.reconciliation(recall["id"])
        consumed_check = next(c for c in recon["checks"] if "消耗" in c["name"])
        self.assertTrue(consumed_check["ok"], consumed_check)
        stock_check = next(c for c in recon["checks"] if "账实" in c["name"])
        self.assertTrue(stock_check["ok"])

        # 失败提交保留为欠件，可按车辆/网点看到；原流水号在补货后重试成功
        self.assertEqual(1, recon["backorder_count"])
        self.s.add_parts("maker", "manufacturer", recall["id"], self.d_cn["id"], 1, 1)
        retry = self.s.report_repair("dealer", "dealer", recall["id"],
                                     "LX00011" if failed_key == "k-b" else "LX00010",
                                     self.d_cn["id"], 1, f"h-{failed_key}", True, idempotency_key=failed_key)
        self.assertEqual("reported", retry["status"])
        attempt = self.s.conn.execute("SELECT attempts,status FROM repair_attempts WHERE idempotency_key=?",
                                      (failed_key,)).fetchone()
        self.assertEqual(2, attempt["attempts"])
        self.assertEqual("accepted", attempt["status"])

        # 成功方重放同一流水号：返回原单，不再扣库存
        win_key = next(k for k, v in results.items() if v[0] == "ok")
        replay = self.s.report_repair("dealer", "dealer", recall["id"],
                                      "LX00011" if win_key == "k-b" else "LX00010",
                                      self.d_cn["id"], 1, f"h-{win_key}", True, idempotency_key=win_key)
        self.assertEqual(results[win_key][1], replay["id"])
        self.assertEqual(0, self.s.conn.execute("SELECT available FROM parts").fetchone()["available"])

    def test_dealer_view_backorder_and_recompute_reason(self):
        """页面按网点看到当前范围版本、欠件、缺口与重算原因。"""
        recall = self.make_recall(countries=("CN", "SG"))
        self.s.register_vehicle("maker", "manufacturer", "LX00020", "X", 2018, "CN", "甲")
        self.s.register_vehicle("maker", "manufacturer", "LX00021", "X", 2018, "CN", "乙")
        self.s.add_parts("maker", "manufacturer", recall["id"], self.d_cn["id"], 1, 1)
        # 第二辆缺件，提交被保留
        self.s.report_repair("dealer", "dealer", recall["id"], "LX00020", self.d_cn["id"],
                             1, "h1", True, idempotency_key="k1")
        with self.assertRaises(ApiError):
            self.s.report_repair("dealer", "dealer", recall["id"], "LX00021", self.d_cn["id"],
                                 1, "h2", True, idempotency_key="k2")
        recon = self.s.reconciliation(recall["id"])
        cn = next(d for d in recon["dealers"] if d["dealer_code"] == "D-CN")
        self.assertEqual(0, cn["available"])       # 唯一一箱已被 k1 用掉
        self.assertEqual(0, cn["ledger_total"])  # 入库 1 消耗 1，台账结余与库存一致
        self.assertEqual(1, cn["backorder_count"])
        self.assertEqual("k2", cn["backorders"][0]["idempotency_key"])
        self.assertEqual(2, cn["demand_in_country"])  # 甲维修已报未确认仍占待办，乙为欠件提交
        self.assertEqual(2, cn["projected_gap"])       # 待办 2 - 库存 0 = 缺口 2
        self.assertIsNotNone(cn["recompute_reason"])
        self.assertTrue(recon["vehicles"][0]["scope_version"] == 1)

    def test_scope_change_marks_recompute_reason_and_report_refresh(self):
        recall = self.make_recall()
        self.s.register_vehicle("maker", "manufacturer", "LX00030", "X", 2018, "CN", "甲")
        self.s.change_scope("maker", "manufacturer", recall["id"],
                            {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN", "SG"]},
                            recall["revision"])
        self.s.register_vehicle("maker", "manufacturer", "LX00031", "X", 2018, "SG", "乙")
        recon = self.s.reconciliation(recall["id"])
        self.assertEqual(2, recon["scope_version"])
        self.assertEqual(2, recon["affected_count"])
        sg_vehicle = next(v for v in recon["vehicles"] if v["vin"] == "LX00031")
        self.assertTrue(sg_vehicle["in_scope"])
        self.assertEqual(2, sg_vehicle["scope_version"])
        self.assertIn("登记", sg_vehicle["recompute_reason"])
        triggers = [r["trigger"] for r in recon["recompute_runs"]]
        self.assertEqual("vehicle_register", triggers[0])
        self.assertIn("scope_change", triggers)
        self.assertEqual("queued", recon["reports"][-1]["status"])
        self.assertTrue(all(c["ok"] for c in recon["checks"]))


if __name__ == "__main__":
    unittest.main()
