"""Four processors, one per assignment part.

A  run_baseline            - no fault tolerance, first failure stops the run
B  run_exception_handling  - try/except + classification + structured log + continue
C  run_retry               - B + the exact retry policy from the fault rules
D  run_fault_tolerant      - C + checkpoint every 5 successes + rollback on DB failure
"""
import time
from dataclasses import dataclass, field

from .errors import (DatabaseFailureError, GatewayTimeoutError, NetworkFailureError,
                     TransactionError)
from .ledger import CheckpointManager, Ledger
from .pipeline import PaymentPipeline


@dataclass
class RetryPolicy:
    """Exact policy derived from the fault rules: up to 2 retries (Retry 1, Retry 2)."""
    max_retries: int = 2
    base_delay_s: float = 0.1       # exponential backoff: 0.1 s, 0.2 s
    factor: float = 2.0
    retry_on: tuple = (NetworkFailureError, GatewayTimeoutError, DatabaseFailureError)

    def delay(self, retry_no):
        return round(self.base_delay_s * self.factor ** (retry_no - 1), 3)

    def is_retryable(self, exc):
        return isinstance(exc, self.retry_on) and exc.transient


@dataclass
class RunResult:
    mode: str
    total: int
    attempted: int = 0
    successful: int = 0
    processed_amount: int = 0
    retries: int = 0
    rollbacks: int = 0
    status: dict = field(default_factory=dict)        # txn_id -> final status
    exception: dict = field(default_factory=dict)     # txn_id -> exception class
    recovery: dict = field(default_factory=dict)      # txn_id -> recovery action
    checkpoints: list = field(default_factory=list)
    ledger_consistent: bool = True
    orphan_rows: list = field(default_factory=list)
    duplicates_suppressed: int = 0
    total_amount: int = 0

    @property
    def lost(self):
        return self.total - self.successful

    @property
    def lost_amount(self):
        return self.total_amount - self.processed_amount

    @property
    def completion_rate(self):
        return self.successful / self.total

    def summary(self):
        return {
            "mode": self.mode,
            "transactions_total": self.total,
            "transactions_attempted": self.attempted,
            "successful": self.successful,
            "lost_or_failed": self.lost,
            "processed_amount_kzt": self.processed_amount,
            "lost_amount_kzt": self.lost_amount,
            "retries": self.retries,
            "rollbacks": self.rollbacks,
            "completion_rate": round(self.completion_rate, 4),
            "ledger_consistent": self.ledger_consistent,
            "orphan_rows": self.orphan_rows,
            "duplicates_suppressed": self.duplicates_suppressed,
            "status": self.status,
        }


def _finish(res, ledger):
    res.successful = ledger.success_count
    res.processed_amount = ledger.total_amount
    res.ledger_consistent = ledger.is_consistent()
    res.orphan_rows = ledger.orphan_rows()
    res.duplicates_suppressed = ledger.duplicates_suppressed
    return res


# ---------------------------------------------------------------- Part A
def baseline_process_all(transactions, pipeline, log):
    """The baseline processor itself: NO try/except. First exception escapes."""
    for txn in transactions:
        log.event("INFO", "txn_start", txn=txn.txn_id, amount=txn.amount)
        pipeline.run(txn)
        log.event("INFO", "txn_success", txn=txn.txn_id)


def run_baseline(transactions, log):
    ledger = Ledger()
    pipeline = PaymentPipeline(ledger)
    res = RunResult("A_baseline", len(transactions), total_amount=sum(t.amount for t in transactions))
    log.event("INFO", "run_start", mode=res.mode, transactions=len(transactions))
    try:
        baseline_process_all(transactions, pipeline, log)
    except TransactionError as exc:              # the *process* crashes here
        log.event("CRITICAL", "processor_crashed", txn=exc.txn_id,
                  exception=type(exc).__name__, message=str(exc))
        crashed_at = exc.txn_id
    else:
        crashed_at = None
    for i, txn in enumerate(transactions):
        if txn.txn_id in ledger.applied:
            res.status[txn.txn_id] = "SUCCESS"
        elif txn.txn_id == crashed_at:
            res.status[txn.txn_id] = "FAILED (crash)"
            res.attempted = i + 1
        else:
            res.status[txn.txn_id] = "NOT PROCESSED"
    if crashed_at is None:
        res.attempted = len(transactions)
    _finish(res, ledger)
    log.event("INFO", "run_end", **{k: v for k, v in res.summary().items() if k != "status"})
    return res


# ---------------------------------------------------------------- Part B
def run_exception_handling(transactions, log):
    ledger = Ledger()
    pipeline = PaymentPipeline(ledger)
    res = RunResult("B_exceptions", len(transactions), total_amount=sum(t.amount for t in transactions))
    log.event("INFO", "run_start", mode=res.mode, transactions=len(transactions))
    for txn in transactions:
        res.attempted += 1
        try:
            pipeline.run(txn)
        except TransactionError as exc:
            res.status[txn.txn_id] = "FAILED"
            res.exception[txn.txn_id] = type(exc).__name__
            res.recovery[txn.txn_id] = "caught, classified, logged; skipped; continue"
            log.event("ERROR", "txn_failed", txn=txn.txn_id, failure=exc.failure_type,
                      exception=type(exc).__name__, stage=exc.stage,
                      transient=exc.transient, action="continue_with_next")
        except Exception as exc:                 # unexpected bug must not kill the batch either
            res.status[txn.txn_id] = "FAILED (unexpected)"
            res.exception[txn.txn_id] = type(exc).__name__
            log.event("ERROR", "txn_failed_unexpected", txn=txn.txn_id, exception=repr(exc))
        else:
            res.status[txn.txn_id] = "SUCCESS"
            log.event("INFO", "txn_success", txn=txn.txn_id, amount=txn.amount)
    _finish(res, ledger)
    log.event("WARNING" if not res.ledger_consistent else "INFO", "run_end",
              **{k: v for k, v in res.summary().items() if k != "status"})
    return res


# ---------------------------------------------------------------- Part C + D
def _run_with_retry(transactions, log, attempt_log, policy, checkpointing, cp_path=None,
                    mode="C_retry", sleep=time.sleep):
    ledger = Ledger()
    pipeline = PaymentPipeline(ledger)
    cpm = CheckpointManager(ledger, interval=5, path=cp_path) if checkpointing else None
    res = RunResult(mode, len(transactions), total_amount=sum(t.amount for t in transactions))
    log.event("INFO", "run_start", mode=mode, max_retries=policy.max_retries,
              backoff=f"{policy.base_delay_s}s x{policy.factor}", checkpoint_interval=5 if cpm else None)

    for txn in transactions:
        res.attempted += 1
        attempt = 0
        while True:
            label = "Initial" if attempt == 0 else f"Retry {attempt}"
            try:
                pipeline.run(txn, attempt)
            except TransactionError as exc:
                exc_name = type(exc).__name__
                res.exception[txn.txn_id] = exc_name
                if policy.is_retryable(exc) and attempt < policy.max_retries:
                    d = policy.delay(attempt + 1)
                    rec = log.event("WARNING", "attempt_failed", txn=txn.txn_id, attempt=label,
                                    exception=exc_name, stage=exc.stage, next=f"retry in {d}s")
                    attempt_log.add(rec, txn, label, exc_name, f"backoff {d}s -> Retry {attempt + 1}", "FAIL")
                    sleep(d)
                    attempt += 1
                    res.retries += 1
                    continue
                # retries exhausted (or permanent error)
                if cpm and isinstance(exc, DatabaseFailureError):
                    info = cpm.rollback()
                    res.rollbacks += 1
                    res.status[txn.txn_id] = "ROLLED BACK"
                    res.recovery[txn.txn_id] = (f"{attempt} retries, then rollback to {info['checkpoint']}")
                    rec = log.event("ERROR", "rollback", txn=txn.txn_id, attempt=label, exception=exc_name,
                                    checkpoint=info["checkpoint"], discarded_rows=info["discarded_rows"],
                                    replayed=info["replayed"], ledger_consistent=ledger.is_consistent())
                    attempt_log.add(rec, txn, label, exc_name,
                                    f"retries exhausted -> rollback to {info['checkpoint']}", "ROLLBACK")
                else:
                    res.status[txn.txn_id] = "FAILED"
                    res.recovery[txn.txn_id] = f"{attempt} retries, marked FAILED, continue"
                    rec = log.event("ERROR", "txn_failed", txn=txn.txn_id, attempt=label, exception=exc_name)
                    attempt_log.add(rec, txn, label, exc_name, "retries exhausted -> mark FAILED", "FAIL")
                break
            else:
                res.status[txn.txn_id] = "SUCCESS" if attempt == 0 else f"SUCCESS after {attempt} retr{'y' if attempt == 1 else 'ies'}"
                if attempt:
                    res.recovery[txn.txn_id] = f"retry x{attempt} (backoff)"
                rec = log.event("INFO", "attempt_success", txn=txn.txn_id, attempt=label, amount=txn.amount,
                                total=ledger.total_amount)
                attempt_log.add(rec, txn, label, res.exception.get(txn.txn_id, "None") if attempt else "None",
                                "Validate -> Process -> Record", "SUCCESS")
                if cpm:
                    cp = cpm.maybe_checkpoint()
                    if cp:
                        log.event("INFO", "checkpoint", name=cp["name"], success_count=cp["success_count"],
                                  total_amount=cp["total_amount"], sha256=cp["sha256"][:12])
                break

    if cpm:
        cp = cpm.final_checkpoint()
        if cp:
            log.event("INFO", "checkpoint", name=cp["name"], trigger=cp["trigger"],
                      success_count=cp["success_count"], total_amount=cp["total_amount"],
                      sha256=cp["sha256"][:12])
        res.checkpoints = [{k: v for k, v in c.items() if k != "state"} for c in cpm.checkpoints]
    _finish(res, ledger)
    log.event("INFO" if res.ledger_consistent else "WARNING", "run_end",
              **{k: v for k, v in res.summary().items() if k != "status"})
    return res


def run_retry(transactions, log, attempt_log, policy=None, sleep=time.sleep):
    return _run_with_retry(transactions, log, attempt_log, policy or RetryPolicy(),
                           checkpointing=False, mode="C_retry", sleep=sleep)


def run_fault_tolerant(transactions, log, attempt_log, cp_path=None, policy=None, sleep=time.sleep):
    return _run_with_retry(transactions, log, attempt_log, policy or RetryPolicy(),
                           checkpointing=True, cp_path=cp_path, mode="D_fault_tolerant", sleep=sleep)
