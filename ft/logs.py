"""Structured (JSON Lines) run log + CSV attempt log. Every line is produced by the run."""
import csv
import json
from datetime import datetime


class RunLog:
    def __init__(self, run_name, jsonl_path=None, echo=True):
        self.run_name = run_name
        self.jsonl_path = jsonl_path
        self.echo = echo
        self.step = 0
        self.events = []
        if jsonl_path:
            open(jsonl_path, "w").close()

    def event(self, level, event, **fields):
        self.step += 1
        rec = {
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "step": self.step,
            "run": self.run_name,
            "level": level,
            "event": event,
            **fields,
        }
        self.events.append(rec)
        if self.jsonl_path:
            with open(self.jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if self.echo:
            extra = " ".join(f"{k}={v}" for k, v in fields.items())
            print(f"[{rec['ts'][11:]}] #{rec['step']:03d} {level:<8} {self.run_name:<18} {event:<22} {extra}")
        return rec


ATTEMPT_FIELDS = ["step", "timestamp", "transaction", "amount_kzt", "attempt", "failure", "action", "result"]


class AttemptLog:
    def __init__(self):
        self.rows = []

    def add(self, log_rec, txn, attempt_label, failure, action, result):
        self.rows.append({
            "step": log_rec["step"],
            "timestamp": log_rec["ts"],
            "transaction": txn.txn_id,
            "amount_kzt": txn.amount,
            "attempt": attempt_label,
            "failure": failure,
            "action": action,
            "result": result,
        })

    def to_csv(self, path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ATTEMPT_FIELDS)
            w.writeheader()
            w.writerows(self.rows)
