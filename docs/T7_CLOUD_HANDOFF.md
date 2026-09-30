# Cloud execution record and reproduction contract

## Historical owner-run acceptance — 2026-09-30

A label-free package was uploaded to a Unity Catalog Volume. The registered MLflow model (numeric version 1) matched the original local scorer on all 862,792 held-out rows. Two real scoring jobs each retained 862,792 unique composite keys and the 8,629-row daily review queue. A separate read-only job recomputed and reconciled business fields with zero differences. It also rejected a changed manifest against the persisted ledger and a nonexistent registry alias, without fallback. Delta table versions did not change during verification.

See [sanitised acceptance JSON](../reports/cloud_run_acceptance.json). Job IDs, account host, billing screenshots, login state and raw notebook outputs are retained privately. The report records owner-observed results; neither the JSON nor public CI independently authenticates those private runs.

The first scoring attempt failed because Spark Connect rejected `.mode('errorifexists')`. Its automatic optimisation retry was cancelled; the ledger was inspected and the output table was absent. The implementation uses `.mode('error')` with the same fail-if-present intention. Corrected job submissions disabled automatic optimisation and timeout retry in addition to `max_retries=0`. The subsequent first/repeat/verify jobs passed. No active AML runs remained at closeout, no recurring schedule was added and no paid upgrade was performed. A stale balance display cannot establish exact job cost.

## Reproduce only in a workspace you control

1. Complete and audit the local experiment. Run `python scripts/prepare_cloud_package.py --output .runtime/cloud-v1`; this packages features without labels, the model and manifests. This is a local operation, not a cloud deployment.
2. Verify the package independently with `scripts/verify_cloud_package.py --help`. Use an isolated environment without the source checkout and pass the frozen score file as the reference. Do not upload labels or private score references.
3. Review `configs/aml_cloud.example.json`. Set your own catalog/schema/Volume, experiment, model and Delta table names. Keep credentials outside this repository. The default `cloud_write_approved: false` intentionally prevents writes.
4. Obtain explicit resource and cost approval. Upload the verified package to your own Volume and import `scripts/aml_cloud_notebook.py` as a Python notebook. Supply the JSON config in the `config` widget. Set the exact manifest hash, and only then set `cloud_write_approved: true`. This gate applies to every action; the verify action itself remains read-only.
5. Submit manual tasks, using register → score → score → verify actions. Use separate saved job outputs, pin the numeric registry version resolved at batch start, and retain single-writer ownership. Set bounded timeouts, `max_retries=0`, `retry_on_timeout=false`, and `disable_auto_optimization=true`; timeout is not a spending cap.
6. Inspect actual task status/output, row counts, composite-key uniqueness, field equality, missing-alias/conflict rejection and unchanged table versions on read-only verification. Check for active runs and review billing separately. Do not report success solely from job submission.

The original operator-specific CLI is excluded; this is a manual reproduction contract, not one-command deployment. `scripts/accept_cloud.py` encodes the original v1 table and queue count; adapt that checker explicitly for a different protocol. CI uses fake evidence only to test rejection logic, never as proof of real cloud execution.
