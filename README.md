# AML Transaction Review

Ranks financial transactions for analyst review under a fixed daily capacity. Trained on the IBM synthetic AML dataset with leakage-safe temporal features, evaluated by review budget rather than AUC, and deployed as a batch scoring job on Databricks.

## Results

Frozen two-day test set: 862,792 transactions, 956 labelled positives (0.11%). The model was selected on validation data only; the test set was never used for tuning. Each day's review queue is capped at a fixed share of that day's transactions.

**Positives found in the daily review queue**

| Daily budget | Queue size | Random review (expected) | Threshold rule | XGBoost | XGBoost recall | XGBoost precision |
|---|---:|---:|---:|---:|---:|---:|
| 1% | 8,629 | 9.6 | 8 | **603** | 63.08% | 6.99% |
| 2% | 17,257 | 19.1 | 452 | **694** | 72.59% | 4.02% |
| 5% | 43,141 | 47.8 | 455 | **819** | 85.67% | 1.90% |

- At a 1% budget, the model finds **63x more positives than random review** with the same analyst capacity.
- The threshold rule (training-set 99th-percentile cut-offs on three account-history features) finds no more than random review at 1%. Its 2% count depends heavily on tied scores: reordering ties alone moves it anywhere between 12 and 956.
- Over the full ranking, average precision is 0.401 for XGBoost and 0.007 for the rule (random ≈ 0.001).

**Why evaluate by budget.** The rule ranks well above random overall (AP 0.007 vs 0.001), yet adds nothing at the 1% cut-off analysts actually work to. A global ranking metric would have hidden this, which is why every result here is reported at a fixed daily review capacity.

**Failure analysis.** All 603 detected positives were ACH payments; the 122 non-ACH positives did not reach the top 1%. Ranking is dominated by the largest payment format, which points to per-format budgets or format-specific models as the next step.

**Databricks deployment.** The model was registered in Unity Catalog with numeric version pinning and used to score the full test set into a Delta batch ledger. I ran the job twice: both runs produced 862,792 unique composite keys, and a read-only reconciliation found zero business-field differences. Guard checks for a missing model alias and a conflicting run manifest also passed.

## What I built

**Data layer**
- Strict positional CSV adapter with stable row identity (source file hash + record number)
- Labels stored separately from features, so they cannot leak into training inputs

**Temporal features**
- Account history computed over `[t-window, t)`, excluding same-minute and future events
- Frozen warm-up / train / validation / test periods split by time

**Modelling and evaluation**
- Category encoding fitted on training data only
- Two pre-declared XGBoost candidates, compared against a threshold rule and random review
- Evaluation at 1%, 2% and 5% daily review budgets, matching how an analyst team works through a queue
- Deterministic tie-breaking with tie-sensitivity bounds, plus failure slices by day, payment format and currency

**Packaging and deployment**
- Hash-checked model bundles and immutable batch outputs
- Rule-based reason context linked back to source rows
- MLflow packaging, Unity Catalog version pinning, Delta batch ledger and replay verification

The budget evaluator is reused from my earlier [ERP Risk MLOps](https://github.com/whitesungun876/erp-risk-mlops) project and vendored here, so no sibling checkout is needed. See [attribution](NOTICE.md).

## Quick start

No data or cloud account required. Use Python 3.12 in a fresh environment:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m unittest discover -s tests -v
```

Or with Docker:

```sh
docker build -t aml-transaction-review:local .
docker run --rm --network none aml-transaction-review:local
```

Tests run on hand-written synthetic fixtures. CI does not download IBM data or access Databricks.

## Documentation

- [Acceptance and verification boundaries](ACCEPTANCE.md)
- [Frozen experiment and failure analysis](reports/AML_EXPERIMENT.md)
- [Data card and licence metadata](docs/AML_DATA_CARD.md)
- [Local reproduction guide](docs/AML_RUNBOOK.md)
- [Cloud execution record](docs/T7_CLOUD_HANDOFF.md)
- [Engineering lessons](docs/ENGINEERING_LESSONS.md)
- [PRD](PRD.md) and [technical design](TECHNICAL_IMPLEMENTATION.md) (original design baselines)

## Scope

- Labels come from IBM's synthetic dataset, not confirmed criminal cases.
- This is a batch prioritisation demo, not a production AML system.
- Scores are ranking margins, not calibrated probabilities. Reason codes are rule context, not model explanations.

Raw data, row-level outputs, trained binaries and private job IDs are not distributed.
