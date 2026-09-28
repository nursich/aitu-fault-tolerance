"""Validate -> Process -> Record pipeline with deterministic fault injection."""
from .data import FAULT_RULES, FAULT_STAGE, Transaction
from .errors import EXCEPTION_BY_TYPE, ValidationError


class FaultInjector:
    """Returns the scripted outcome of attempt N for a transaction (from FAULT_RULES)."""

    def __init__(self, rules=None):
        self.rules = rules or FAULT_RULES

    def should_fail(self, txn: Transaction, attempt: int) -> bool:
        script = self.rules[txn.failure_type]
        # attempts beyond the script repeat the last outcome
        outcome = script[attempt] if attempt < len(script) else script[-1]
        return not outcome


class PaymentPipeline:
    def __init__(self, ledger, injector=None):
        self.ledger = ledger
        self.injector = injector or FaultInjector()

    def _maybe_fail(self, txn, stage, attempt):
        if txn.failure_type != "None" and FAULT_STAGE[txn.failure_type] == stage \
                and self.injector.should_fail(txn, attempt):
            raise EXCEPTION_BY_TYPE[txn.failure_type](txn.txn_id, stage, attempt)

    def validate(self, txn, attempt):
        if txn.amount <= 0 or not txn.txn_id.startswith("T"):
            raise ValidationError(txn.txn_id, "validate", attempt)

    def process(self, txn, attempt):
        self._maybe_fail(txn, "process", attempt)

    def record(self, txn, attempt):
        # write 1: payment row (idempotent on txn_id)
        self.ledger.insert_row(txn)
        # a Database fault strikes here -> orphan row, aggregates not updated
        self._maybe_fail(txn, "record", attempt)
        # write 2: aggregates (idempotent on txn_id)
        self.ledger.apply(txn.txn_id)

    def run(self, txn, attempt=0):
        self.validate(txn, attempt)
        self.process(txn, attempt)
        self.record(txn, attempt)
