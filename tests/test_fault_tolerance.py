"""Unit tests.   python -m unittest discover -s tests -v"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ft.data import TRANSACTIONS, Transaction                                  # noqa: E402
from ft.errors import ValidationError                                          # noqa: E402
from ft.ledger import CheckpointManager, Ledger                               # noqa: E402
from ft.logs import AttemptLog, RunLog                                         # noqa: E402
from ft.pipeline import PaymentPipeline                                        # noqa: E402
from ft.processors import (RetryPolicy, run_baseline, run_exception_handling,  # noqa: E402
                           run_fault_tolerant, run_retry)

NO_SLEEP = lambda s: None  # noqa: E731


def log():
    return RunLog("test", echo=False)


class PartA(unittest.TestCase):
    def test_baseline_stops_at_first_failure(self):
        r = run_baseline(TRANSACTIONS, log())
        self.assertEqual(r.attempted, 2)
        self.assertEqual(r.successful, 1)
        self.assertEqual(r.lost, 14)
        self.assertEqual(r.processed_amount, 12000)
        self.assertEqual(r.lost_amount, 425000)


class PartB(unittest.TestCase):
    def test_processing_continues_after_failure(self):
        r = run_exception_handling(TRANSACTIONS, log())
        self.assertEqual(r.attempted, 15)
        self.assertEqual(r.successful, 8)
        self.assertEqual(r.exception["T006"], "DatabaseFailureError")
        self.assertEqual(r.exception["T004"], "GatewayTimeoutError")
        # without rollback a DB failure leaves orphan rows -> inconsistent ledger
        self.assertFalse(r.ledger_consistent)
        self.assertEqual(r.orphan_rows, ["T006", "T012"])


class PartC(unittest.TestCase):
    def test_exact_retry_policy(self):
        att = AttemptLog()
        r = run_retry(TRANSACTIONS, log(), att, sleep=NO_SLEEP)
        per_txn = {}
        for row in att.rows:
            per_txn.setdefault(row["transaction"], []).append(row["result"])
        self.assertEqual(per_txn["T002"], ["FAIL", "SUCCESS"])            # Network
        self.assertEqual(per_txn["T004"], ["FAIL", "FAIL", "SUCCESS"])    # Timeout
        self.assertEqual(per_txn["T006"], ["FAIL", "FAIL", "FAIL"])       # Database
        self.assertEqual(r.retries, 11)
        self.assertEqual(r.successful, 13)

    def test_validation_error_is_not_retried(self):
        bad = [Transaction("T999", -5, "None")]
        att = AttemptLog()
        r = run_retry(bad, log(), att, sleep=NO_SLEEP)
        self.assertEqual(r.retries, 0)
        self.assertEqual(r.exception["T999"], ValidationError.__name__)

    def test_backoff(self):
        p = RetryPolicy()
        self.assertEqual([p.delay(1), p.delay(2)], [0.1, 0.2])


class PartD(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_checkpoints_and_rollbacks(self):
        r = run_fault_tolerant(TRANSACTIONS, log(), AttemptLog(), cp_path=os.path.join(self.tmp, "cp.json"),
                               sleep=NO_SLEEP)
        cps = {c["name"]: (c["success_count"], c["total_amount"]) for c in r.checkpoints}
        self.assertEqual(cps["CP1"], (5, 103000))
        self.assertEqual(cps["CP2"], (10, 214000))
        self.assertEqual(cps["CP3"], (13, 287000))
        self.assertEqual(r.rollbacks, 2)
        self.assertEqual(r.status["T006"], "ROLLED BACK")
        self.assertTrue(r.ledger_consistent)
        self.assertEqual(r.processed_amount, 287000)

    def test_rollback_does_not_duplicate_checkpoint(self):
        """Guard: a rollback must never create a duplicate checkpoint."""
        r = run_fault_tolerant(TRANSACTIONS, log(), AttemptLog(), sleep=NO_SLEEP)
        counts = [c["success_count"] for c in r.checkpoints]
        self.assertEqual(len(counts), len(set(counts)))

    def test_rollback_replays_journal(self):
        """Work committed after the checkpoint survives the rollback (redo journal)."""
        txns = [Transaction(f"T10{i}", 1000, "None") for i in range(5)] + \
               [Transaction("T105", 2000, "None"), Transaction("T106", 3000, "Database")]
        r = run_fault_tolerant(txns, log(), AttemptLog(), sleep=NO_SLEEP)
        self.assertEqual(r.status["T105"], "SUCCESS")
        self.assertEqual(r.processed_amount, 7000)
        self.assertTrue(r.ledger_consistent)

    def test_corrupted_checkpoint_is_skipped(self):
        ledger = Ledger()
        cpm = CheckpointManager(ledger, interval=1)
        pipe = PaymentPipeline(ledger)
        pipe.run(Transaction("T001", 12000, "None"))
        cpm.maybe_checkpoint()
        cpm.checkpoints[-1]["state"]["total_amount"] = 999   # corrupt CP1
        self.assertEqual(cpm.latest_valid()["name"], "CP0")

    def test_idempotent_record(self):
        ledger = Ledger()
        pipe = PaymentPipeline(ledger)
        t = Transaction("T001", 12000, "None")
        pipe.run(t)
        pipe.run(t)                     # duplicate delivery of the same payment
        self.assertEqual(ledger.total_amount, 12000)
        self.assertEqual(ledger.duplicates_suppressed, 2)


if __name__ == "__main__":
    unittest.main()
