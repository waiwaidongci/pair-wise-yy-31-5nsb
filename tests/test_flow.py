import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, RecallService, Store


class RecallFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.s = RecallService(Store(Path(self.tmp.name) / "r.db"))
        self.dealer_cn = self.s.register_dealer("reg", "regulator", "D-CN", "中国中心", "CN")
        self.dealer_sg = self.s.register_dealer("reg", "regulator", "D-SG", "新加坡中心", "SG")

    def tearDown(self): self.s.store.close(); self.tmp.cleanup()

    def make_recall(self):
        r = self.s.create_recall("maker", "manufacturer", "RC-1", "制动检查", {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN"]}, {"version": 1, "description": "更换软管"})
        r = self.s.submit_recall("maker", "manufacturer", r["id"], r["revision"])
        return self.s.review_recall("reg", "regulator", r["id"], "publish", r["revision"], "同意发布")

    def test_publish_cross_border_repair_scope_change_and_unfinished(self):
        recall = self.make_recall()
        vehicle = self.s.register_vehicle("maker", "manufacturer", "LX00001", "X", 2018, "CN", "张三")
        self.s.register_vehicle("maker", "manufacturer", "LX00002", "X", 2018, "CN", "李四")
        self.s.transfer_vehicle("dealer", "dealer", vehicle["vin"], "SG", "Wang")
        self.s.add_parts("maker", "manufacturer", recall["id"], self.dealer_sg["id"], 1, 2)
        report = self.s.report_repair("dealer", "dealer", recall["id"], vehicle["vin"], self.dealer_sg["id"], 1, "abc123", True, "BP-9", "repair-1")
        self.assertEqual("reported", report["status"])
        confirmed = self.s.review_repair("reg", "regulator", report["id"], "confirm", "证据一致")
        self.assertEqual("confirmed", confirmed["status"])
        before = self.s.unfinished("reg", "regulator", recall["id"])
        self.assertEqual(1, before["unfinished_count"])
        changed = self.s.change_scope("maker", "manufacturer", recall["id"], {"models": ["X"], "model_years": [2018], "vin_prefixes": ["LX"], "countries": ["CN"]}, recall["revision"])
        self.assertEqual(2, changed["scope_version"])
        detail = self.s.recall_detail(recall["id"])
        self.assertEqual(2, len(detail["reports"]))
        self.assertEqual("confirmed", detail["repairs"][0]["status"])

    def test_shortage_wrong_remedy_permissions_and_duplicate(self):
        recall = self.make_recall()
        v = self.s.register_vehicle("maker", "manufacturer", "LX10000", "X", 2018, "CN", "赵六")
        with self.assertRaises(ApiError):
            self.s.review_recall("dealer", "dealer", recall["id"], "publish", recall["revision"])
        self.s.add_parts("maker", "manufacturer", recall["id"], self.dealer_cn["id"], 1, 1)
        with self.assertRaises(ApiError) as ctx:
            self.s.report_repair("dealer", "dealer", recall["id"], v["vin"], self.dealer_cn["id"], 2, "bad", True, idempotency_key="wrong")
        self.assertIn("版本", ctx.exception.message)
        first = self.s.report_repair("dealer", "dealer", recall["id"], v["vin"], self.dealer_cn["id"], 1, "ok", False, idempotency_key="idem")
        duplicate = self.s.report_repair("dealer", "dealer", recall["id"], v["vin"], self.dealer_cn["id"], 1, "ok", False, idempotency_key="idem")
        self.assertEqual(first["id"], duplicate["id"])
        with self.assertRaises(ApiError):
            self.s.report_repair("dealer", "dealer", recall["id"], v["vin"], self.dealer_cn["id"], 1, "ok", True, idempotency_key="other")
        flagged = self.s.review_repair("reg", "regulator", first["id"], "confirm")
        self.assertEqual("flagged", flagged["status"])


if __name__ == "__main__": unittest.main()
