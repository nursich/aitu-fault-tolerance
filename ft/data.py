"""Input data for Assignment 2 (synthetic educational dataset, AITU context).

Every transaction follows the pipeline  Validate -> Process -> Record.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Transaction:
    txn_id: str
    amount: int          # KZT
    failure_type: str    # "None" | "Network" | "Timeout" | "Database"


TRANSACTIONS = [
    Transaction("T001", 12000, "None"),
    Transaction("T002", 25000, "Network"),
    Transaction("T003", 8000, "None"),
    Transaction("T004", 45000, "Timeout"),
    Transaction("T005", 13000, "None"),
    Transaction("T006", 70000, "Database"),
    Transaction("T007", 9000, "None"),
    Transaction("T008", 31000, "Network"),
    Transaction("T009", 15000, "None"),
    Transaction("T010", 50000, "Timeout"),
    Transaction("T011", 6000, "None"),
    Transaction("T012", 80000, "Database"),
    Transaction("T013", 11000, "None"),
    Transaction("T014", 22000, "None"),
    Transaction("T015", 40000, "Network"),
]

# Fault rules exactly as given in the assignment.
# Index 0 = initial attempt, 1 = retry 1, 2 = retry 2.
# True = attempt succeeds, False = attempt fails.
FAULT_RULES = {
    "None":     [True],
    "Network":  [False, True],          # Fail -> Success
    "Timeout":  [False, False, True],   # Fail -> Fail -> Success
    "Database": [False, False, False],  # Fail -> Fail -> Fail -> Rollback
}

# Pipeline stage in which each fault type is injected.
FAULT_STAGE = {
    "Network": "process",   # payment gateway call (bank / Kaspi / Halyk) over the network
    "Timeout": "process",   # gateway does not answer inside the deadline
    "Database": "record",   # commit to the student-payments ledger fails
}
