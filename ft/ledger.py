"""In-memory student-payments ledger with idempotent writes, checkpoints and rollback.

The Record stage performs two writes (like a real DB without a wrapping transaction):
  1) insert the payment row           -> rows[txn_id] = amount
  2) apply it to the aggregates       -> success_count, total_amount
A Database fault is injected *between* the two writes, so a failed Record leaves a
partial (orphan) row. That is exactly the state a rollback has to clean up.
"""
import hashlib
import json
import os
from copy import deepcopy


class Ledger:
    def __init__(self):
        self.rows = {}            # payment records table: txn_id -> amount
        self.applied = []         # txn_ids applied to aggregates, in order
        self.success_count = 0
        self.total_amount = 0
        self.duplicates_suppressed = 0
        self.journal = []         # redo journal: txns applied since the latest checkpoint

    # ---- writes -------------------------------------------------------
    def insert_row(self, txn):
        """Idempotent insert keyed by txn_id (idempotency key)."""
        if txn.txn_id in self.rows:
            self.duplicates_suppressed += 1
            return False
        self.rows[txn.txn_id] = txn.amount
        return True

    def apply(self, txn_id):
        """Idempotent aggregate update. Returns False if already applied."""
        if txn_id in self.applied:
            self.duplicates_suppressed += 1
            return False
        self.applied.append(txn_id)
        self.success_count += 1
        self.total_amount += self.rows[txn_id]
        self.journal.append((txn_id, self.rows[txn_id]))
        return True

    # ---- consistency --------------------------------------------------
    def orphan_rows(self):
        return sorted(set(self.rows) - set(self.applied))

    def is_consistent(self):
        return (not self.orphan_rows()
                and self.success_count == len(self.applied)
                and self.total_amount == sum(self.rows[t] for t in self.applied))

    # ---- snapshot / restore ------------------------------------------
    def state(self):
        return {
            "applied": list(self.applied),
            "rows": dict(self.rows),
            "success_count": self.success_count,
            "total_amount": self.total_amount,
        }

    def load_state(self, state):
        self.applied = list(state["applied"])
        self.rows = dict(state["rows"])
        self.success_count = state["success_count"]
        self.total_amount = state["total_amount"]


def state_hash(state):
    payload = json.dumps(state, sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


class CheckpointManager:
    """Creates a checkpoint after every `interval` successful transactions.

    Design decisions (differ from the simplest solution):
      * a genesis checkpoint CP0 (empty state) exists, so a DB failure before CP1
        still has a valid rollback target;
      * each checkpoint stores a SHA-256 of its state; restore() walks back to the
        latest checkpoint whose hash still verifies ("latest *valid* checkpoint");
      * after restoring, the redo journal (transactions committed after the
        checkpoint) is replayed idempotently, so rollback never loses good work;
      * checkpoints are persisted to JSON with an atomic write (tmp + os.replace).
    """

    def __init__(self, ledger, interval=5, path=None):
        self.ledger = ledger
        self.interval = interval
        self.path = path
        self.checkpoints = []
        self.last_cp_success_count = 0
        self._create("CP0", trigger="genesis")

    def _create(self, name, trigger):
        state = self.ledger.state()
        cp = {
            "name": name,
            "trigger": trigger,
            "success_count": state["success_count"],
            "total_amount": state["total_amount"],
            "committed_ids": state["applied"],
            "state": deepcopy(state),
            "sha256": state_hash(state),
        }
        self.checkpoints.append(cp)
        self.ledger.journal = []                     # journal restarts at each checkpoint
        self.last_cp_success_count = state["success_count"]
        self._persist()
        return cp

    def maybe_checkpoint(self):
        """Call only right after a NEW successful commit."""
        if self.ledger.success_count - self.last_cp_success_count >= self.interval:
            return self._create(f"CP{len(self.checkpoints)}", trigger=f"every {self.interval} successes")
        return None

    def final_checkpoint(self):
        if self.ledger.success_count != self.last_cp_success_count:
            return self._create(f"CP{len(self.checkpoints)}", trigger="end-of-batch flush")
        return None

    def latest_valid(self):
        for cp in reversed(self.checkpoints):
            if state_hash(cp["state"]) == cp["sha256"]:
                return cp
        raise RuntimeError("no valid checkpoint")

    def rollback(self):
        """Restore latest valid checkpoint, then replay the redo journal. Returns info dict."""
        cp = self.latest_valid()
        journal = list(self.ledger.journal)
        discarded = self.ledger.orphan_rows()
        self.ledger.load_state(deepcopy(cp["state"]))
        self.ledger.journal = []                     # apply() below rebuilds it
        replayed = []
        for txn_id, amount in journal:
            self.ledger.rows[txn_id] = amount
            if self.ledger.apply(txn_id):
                replayed.append(txn_id)
        return {"checkpoint": cp["name"], "discarded_rows": discarded, "replayed": replayed}

    def _persist(self):
        if not self.path:
            return
        public = [{k: v for k, v in cp.items() if k != "state"} for cp in self.checkpoints]
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(public, f, indent=2)
        os.replace(tmp, self.path)
