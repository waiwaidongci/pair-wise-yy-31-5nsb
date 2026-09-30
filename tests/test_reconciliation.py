import sys, tempfile, threading, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, RecallService, Store


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = RecallService(Store(Path(self.tmp.name) / "r.db"))
        self.dealer = self.s.register_dealer("reg", "regulator", "D-1", "中心", "CN")

    def tearDown(self):
        self.s.store.close()
        self.tmp.cleanup()

    def _publish(self):
        r = self.s.create_recall("maker", "manufacturer", "RC-1", "制动检查",
                                 {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN"]},
                                 {"version": 1, "description": "更换软管"})
        r = self.s.submit_recall("maker", "manufacturer", r["id"], r["revision"])
        return self.s.review_recall("reg", "regulator", r["id"], "publish", r["revision"], "同意")

    def test_last_part_only_one_repair_and_retry_same_key(self):
        recall = self._publish()
        v1 = self.s.register_vehicle("maker", "manufacturer", "LX10001", "X", 2018, "CN", "张三")
        v2 = self.s.register_vehicle("maker", "manufacturer", "LX10002", "X", 2018, "CN", "李四")
        self.s.add_parts("maker", "manufacturer", recall["id"], self.dealer["id"], 1, 1)

        results = {}
        def do(vin, key):
            try:
                results[key] = ("ok", self.s.report_repair("dealer", "dealer", recall["id"], vin, self.dealer["id"], 1, "hash-" + key, True, idempotency_key=key))
            except ApiError as exc:
                results[key] = ("err", exc.status, exc.message)

        t1 = threading.Thread(target=do, args=(v1["vin"], "k1"))
        t2 = threading.Thread(target=do, args=(v2["vin"], "k2"))
        t1.start(); t2.start(); t1.join(); t2.join()

        oks = [k for k, v in results.items() if v[0] == "ok"]
        errs = [k for k, v in results.items() if v[0] == "err"]
        self.assertEqual(1, len(oks), f"只能有一笔维修成功: {results}")
        self.assertEqual(1, len(errs), f"另一笔必须失败: {results}")
        self.assertEqual(409, results[errs[0]][1])

        # 库存只被扣一次，不能出现扣了库存却没有维修单
        parts = self.s.conn.execute("SELECT available FROM parts WHERE recall_id=? AND dealer_id=?", (recall["id"], self.dealer["id"])).fetchone()
        self.assertEqual(0, parts["available"])
        repairs = self.s.conn.execute("SELECT COUNT(*) c FROM repairs WHERE recall_id=?", (recall["id"],)).fetchone()["c"]
        self.assertEqual(1, repairs)

        # 失败后按同一流水号在同一车辆上重试：确定性结果，不重复建单
        failed_key = errs[0]
        failed_vin = v1["vin"] if failed_key == "k1" else v2["vin"]
        with self.assertRaises(ApiError) as ctx:
            self.s.report_repair("dealer", "dealer", recall["id"], failed_vin, self.dealer["id"], 1, "hash-" + failed_key, True, idempotency_key=failed_key)
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("零件", ctx.exception.message)
        repairs2 = self.s.conn.execute("SELECT COUNT(*) c FROM repairs WHERE recall_id=?", (recall["id"],)).fetchone()["c"]
        self.assertEqual(1, repairs2)

        # 幂等：同一流水号重复提交返回同一笔维修，不重复扣库存
        ok_key = oks[0]
        ok_vin = v1["vin"] if ok_key == "k1" else v2["vin"]
        again = self.s.report_repair("dealer", "dealer", recall["id"], ok_vin, self.dealer["id"], 1, "hash-" + ok_key, True, idempotency_key=ok_key)
        self.assertEqual(ok_key, again["idempotency_key"])
        self.assertEqual(1, self.s.conn.execute("SELECT COUNT(*) c FROM repairs WHERE recall_id=?", (recall["id"],)).fetchone()["c"])


class RecalculationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = RecallService(Store(Path(self.tmp.name) / "r.db"))
        self.dealer_cn = self.s.register_dealer("reg", "regulator", "D-CN", "中国中心", "CN")
        self.dealer_sg = self.s.register_dealer("reg", "regulator", "D-SG", "新加坡中心", "SG")

    def tearDown(self):
        self.s.store.close()
        self.tmp.cleanup()

    def _publish(self):
        r = self.s.create_recall("maker", "manufacturer", "RC-2", "制动检查",
                                 {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN"]},
                                 {"version": 1, "description": "更换软管"})
        r = self.s.submit_recall("maker", "manufacturer", r["id"], r["revision"])
        return self.s.review_recall("reg", "regulator", r["id"], "publish", r["revision"], "同意")

    def test_scope_change_recalculates_and_keeps_history(self):
        recall = self._publish()
        v = self.s.register_vehicle("maker", "manufacturer", "LX20001", "X", 2018, "CN", "王五")
        self.s.add_parts("maker", "manufacturer", recall["id"], self.dealer_cn["id"], 1, 5)
        report = self.s.report_repair("dealer", "dealer", recall["id"], v["vin"], self.dealer_cn["id"], 1, "h1", True, idempotency_key="r1")
        self.s.review_repair("reg", "regulator", report["id"], "confirm")

        # 发布时已生成 v1 通知与待办
        detail = self.s.recall_detail(recall["id"])
        self.assertEqual(1, len(detail["notifications"]))
        self.assertEqual("done", detail["todos"][0]["status"])

        # 范围调整：新增 SG 市场，版本升到 2
        changed = self.s.change_scope("maker", "manufacturer", recall["id"],
                                      {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN", "SG"]},
                                      recall["revision"])
        self.assertEqual(2, changed["scope_version"])

        detail = self.s.recall_detail(recall["id"])
        # 历史通知保留（v1 + v2 两条）
        self.assertEqual(2, len(detail["notifications"]))
        self.assertEqual([1, 2], sorted(n["scope_version"] for n in detail["notifications"]))
        # 历史上报保留（v1 + v2 两条）
        self.assertEqual(2, len(detail["reports"]))
        # 已确认维修保留
        self.assertEqual(1, len(detail["repairs"]))
        self.assertEqual("confirmed", detail["repairs"][0]["status"])
        # 待办：v1 done（历史），v2 done（维修已确认）
        self.assertEqual(2, len(detail["todos"]))
        self.assertEqual({1: "done", 2: "done"}, {t["scope_version"]: t["status"] for t in detail["todos"]})
        # 重算原因可查
        self.assertEqual("scope_changed", detail["recalculations"][-1]["trigger"])

    def test_transfer_recalculates_todo_and_notification(self):
        recall = self._publish()
        v = self.s.register_vehicle("maker", "manufacturer", "LX30001", "X", 2018, "CN", "赵六")
        # 发布后待办分配给 CN 网点
        detail = self.s.recall_detail(recall["id"])
        self.assertEqual(1, detail["todos"][0]["dealer_id"])
        self.assertEqual("open", detail["todos"][0]["status"])

        # 车辆转手到 SG：所在国一变，重算受影响待办与通知
        self.s.transfer_vehicle("dealer", "dealer", v["vin"], "SG", "Wang")
        detail = self.s.recall_detail(recall["id"])
        # 通知仍为当前范围版本（origin_country=CN 使其仍在范围内）
        self.assertEqual(1, len(detail["notifications"]))
        self.assertEqual(1, detail["notifications"][0]["scope_version"])
        # 待办重新分配给 SG 网点
        current_todo = max(detail["todos"], key=lambda t: t["scope_version"])
        self.assertEqual(self.dealer_sg["id"], current_todo["dealer_id"])
        self.assertEqual("vehicle_transferred", detail["recalculations"][-1]["trigger"])
        self.assertEqual(v["id"], detail["recalculations"][-1]["vehicle_id"])

    def test_reconciliation_shows_scope_shortage_and_reasons(self):
        recall = self._publish()
        v = self.s.register_vehicle("maker", "manufacturer", "LX40001", "X", 2018, "CN", "孙七")
        # 不给 SG 网点备件，先把车辆转到 SG
        self.s.transfer_vehicle("dealer", "dealer", v["vin"], "SG", "Wang")
        recon = self.s.reconciliation(recall["id"])
        # 车辆视图：当前范围版本、欠件标记
        vv = next(x for x in recon["vehicles"] if x["vin"] == v["vin"])
        self.assertTrue(vv["in_scope"])
        self.assertEqual(1, vv["current_scope_version"])
        self.assertTrue(vv["shortage"], "SG 网点无备件，应标记欠件")
        # 网点视图：欠件车辆
        dv = next(x for x in recon["dealers"] if x["dealer_id"] == self.dealer_sg["id"])
        self.assertEqual(1, dv["shortage_count"])
        self.assertIn(v["vin"], dv["shortage_vins"])
        # 重算原因可查
        self.assertEqual("vehicle_transferred", recon["recalculations"][-1]["trigger"])
        # 监管上报已接入
        self.assertEqual(1, len(recon["regulatory_reports"]))


if __name__ == "__main__":
    unittest.main()
