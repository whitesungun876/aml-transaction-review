# AML Transaction Review

Batch review prioritisation on **IBM synthetic financial transactions**, with temporal features, budget-based evaluation and a Databricks deployment demonstration. A personal portfolio project developed with Codex assistance—not a production AML system or a bank engagement.

## Results at a glance

The frozen two-day test contains **862,792 transactions / 956 labelled positives**. At a daily 1% review budget, XGBoost selected 8,629 transactions and recovered **603 positives (63.08% recall, 6.99% precision)**. The model was selected using validation data, not test performance.

The limitation matters: every detected positive was ACH; all 122 non-ACH positives were missed at this budget. About 93% of selected transactions were label-negative. These are synthetic transaction labels, not independently verified criminal cases.

On 2026-09-30, owner-run Databricks jobs registered a Unity Catalog model and scored the full held-out set into Delta twice. Both runs retained 862,792 unique composite keys; a separate read-only reconciliation found zero business-field differences. Missing-alias and conflicting-manifest checks also passed. This assumes a single writer and is not a distributed-concurrency or production-SLA claim.

## Quick start — no data or cloud required

Use Python 3.12 in a fresh environment:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m unittest discover -s tests -v
```

Or use Docker:

```sh
docker build -t aml-transaction-review:local .
docker run --rm --network none aml-transaction-review:local
```

Tests use hand-written synthetic fixtures. CI neither downloads IBM data nor accesses Databricks. Direct runtime dependencies are version-pinned; this is not a complete transitive lockfile.

## What is implemented

- Strict positional CSV adapter, source-hash/record-number identities and separate labels.
- Historical features using `[t-window, t)`; same-minute and future events excluded.
- Frozen warm-up/train/validation/test periods, train-only category encoding, two XGBoost candidates and a rule baseline.
- Daily 1/2/5% review budgets, deterministic tie-breaking, tie sensitivity and failure slices.
- Hash-checked model bundles, immutable local batches and source-linked rule context.
- MLflow packaging, numeric Unity Catalog version pinning, Delta batch ledger and replay verification.

The generic budget evaluator is vendored from the author's ERP project; this repository does **not** require a sibling checkout or local ERP Docker image. See [attribution](NOTICE.md).

## Evidence and documentation

- [Acceptance and verification boundaries](ACCEPTANCE.md)
- [Frozen experiment and failure analysis](reports/AML_EXPERIMENT.md)
- [Data card and licence metadata](docs/AML_DATA_CARD.md)
- [Local reproduction guide](docs/AML_RUNBOOK.md)
- [Cloud execution record and reproduction contract](docs/T7_CLOUD_HANDOFF.md)
- [Reusable engineering lessons](docs/ENGINEERING_LESSONS.md)
- [PRD](PRD.md) and [technical design](TECHNICAL_IMPLEMENTATION.md): original design baselines, not claims that every proposed extension shipped.

Historical metrics precede the standalone packaging changes. No test-set retuning or new full-data training was performed for publication. Private job IDs, billing/account information, raw data, row-level outputs, trained binaries and personal application materials are not distributed. Public summaries are owner-run evidence, not independent third-party validation. Scores are raw margins, not calibrated probabilities; reason codes are rule context, not SHAP or causal explanations.
