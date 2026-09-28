# Assignment 2 — Software Fault-Tolerant Design
AITU Student Scholarship and Tuition Payment Processing (synthetic data)

Pipeline per transaction: **Validate → Process → Record**. Python 3.9+ standard library only.

## Run
```bash
python run_all.py                            # runs Parts A–D, writes logs/ and results/
python -m unittest discover -s tests -v      # 10 unit tests
```

## Structure
| File | Content |
|---|---|
| `ft/data.py` | 15 transactions + the exact fault rules (Network / Timeout / Database) |
| `ft/errors.py` | failure classification: `NetworkFailureError`, `GatewayTimeoutError`, `DatabaseFailureError`, `ValidationError` |
| `ft/pipeline.py` | Validate → Process → Record, deterministic `FaultInjector` driven by the fault rules |
| `ft/ledger.py` | idempotent ledger, `CheckpointManager` (every 5 successes, SHA-256 validated, redo-journal replay) |
| `ft/processors.py` | Part A `run_baseline`, Part B `run_exception_handling`, Part C `run_retry`, Part D `run_fault_tolerant` |
| `ft/logs.py` | JSON-Lines structured log + CSV attempt log |
| `run_all.py` | runs everything, computes Part E |
| `tests/` | unit tests incl. regression test for the duplicate-checkpoint bug |

## Generated evidence
`logs/part*_*.jsonl`, `logs/partC_attempt_log.csv`, `logs/partD_attempt_log.csv`,
`results/checkpoints.json`, `results/transactions_final.csv`, `results/metrics.json`, `results/before_after.md`.

## Key results (Part E)
| Metric | Baseline | Fault-tolerant |
|---|---|---|
| Successful | 1 | 13 |
| Failed / lost | 14 | 2 (T006, T012 rolled back) |
| Lost amount, KZT | 425,000 | 150,000 |
| Retries | 0 | 11 |
| Rollbacks | 0 | 2 |
| Completion rate | 6.67% | 86.67% |
