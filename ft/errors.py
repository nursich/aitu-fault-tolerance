"""Failure classification: one exception class per failure type."""


class TransactionError(Exception):
    """Base class for all transaction faults."""
    failure_type = "Unknown"
    transient = False      # may a retry help?

    def __init__(self, txn_id: str, stage: str, attempt: int):
        self.txn_id = txn_id
        self.stage = stage
        self.attempt = attempt
        super().__init__(f"{self.failure_type} failure in {stage} stage of {txn_id} (attempt {attempt})")


class ValidationError(TransactionError):
    """Invalid input (e.g. non-positive amount). Permanent: never retried."""
    failure_type = "Validation"
    transient = False


class NetworkFailureError(TransactionError):
    failure_type = "Network"
    transient = True


class GatewayTimeoutError(TransactionError):
    failure_type = "Timeout"
    transient = True


class DatabaseFailureError(TransactionError):
    failure_type = "Database"
    transient = True       # retried per the fault rules, then escalated to rollback


EXCEPTION_BY_TYPE = {
    "Network": NetworkFailureError,
    "Timeout": GatewayTimeoutError,
    "Database": DatabaseFailureError,
}
