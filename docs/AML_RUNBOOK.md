# Local reproduction guide

Run from the repository root. Install with `python -m pip install -e .` in Python 3.12 or build the standalone Dockerfile. No ERP repository or pre-existing ERP image is required.

## Synthetic checks

```sh
python scripts/check_local.py
python scripts/train_aml.py --help
```

## Full-data experiment

The following downloads roughly 476 MB from the public provider and requires additional disk/memory for Parquet, feature sorting and model training. Read the data card and provider terms first. It does not contact Databricks. Use a clean checkout/runtime; existing frozen outputs deliberately reject overwrites.

```sh
python scripts/acquire.py
python scripts/prepare_aml.py --source .runtime/raw/HI-Small_Trans.csv --manifest .runtime/raw/source_manifest.json --output .runtime/prepared-v1
python scripts/train_aml.py --prepared .runtime/prepared-v1 --output .runtime/experiment-v1
python scripts/score_aml.py --features .runtime/prepared-v1/features.parquet --experiment .runtime/experiment-v1 --output .runtime/batches --batch-id hi-small-test-v1
python scripts/audit_outputs.py
python scripts/track_experiment.py --experiment .runtime/experiment-v1 --tracking-dir .runtime/mlflow
```

Docker users can prefix these commands with `docker run --rm -v "$PWD:/work" aml-transaction-review:local`. Only acquisition needs external data access. Do not substitute random row sampling for complete temporal history.

Feature reproducibility checking additionally requires a separately rebuilt `.runtime/reproducibility/features.parquet` from the same transactions and configuration using `aml_risk.features.build`; `scripts/check_reproducibility.py` compares logical values and order. It is not part of the small-fixture test suite.

The original frozen experiment's metrics are historical. Vendoring the evaluator and changing packaging alters the code fingerprint; a fresh run must retain its own manifests and evidence. Do not reuse the published model hash as if it came from a new run. Do not repeatedly tune against this already-observed test set.

## Recovery and output interpretation

Any quarantined input prevents an adapter COMPLETE marker. Inspect the quality report and retry into a new directory; never manually add success markers. Scoring with an existing batch identity checks content before replay; changed inputs require a new identity. Outputs and local MLflow storage remain in ignored `.runtime/`.

Source references use a file SHA plus 1-based CSV logical record number, not a bank-issued transaction ID or physical text line. Scores are raw margins. Reason codes reflect training-quantile rule context, not model attribution.

Cloud access and costs require separate approval and configuration: [cloud guide](T7_CLOUD_HANDOFF.md). The private operator CLI is intentionally not published. `scripts/accept_cloud.py` is the historical run-specific evidence checker (fixed table/queue expectations); it needs private saved job-output and closeout files and is not a generic cloud launcher.
