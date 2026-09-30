# Acceptance record

Recorded 2026-09-30. This separates historical full-data/cloud evidence from reproducible public tests.

| Scope | Observed result | Evidence / boundary |
|---|---|---|
| Adapter | 5,078,345 accepted rows, 0 quarantined; 5,077,237 in declared observation interval | Data card; source hash pinned, raw data private |
| Temporal features | Full rebuild matched all logical values and ordering | `reports/feature_reproducibility.json`; physical Parquet encodings can differ |
| Selection | Two candidates compared on validation; candidate 0 selected before test evaluation | Frozen experiment report; no publication-time retraining |
| Held-out evaluation | 603/956 positives recovered in 8,629 daily-budget selections | `reports/output_audit.json`; failure slices disclosed |
| Local batches | Fresh scoring exact match, 0 source-reference mismatches | Aggregate output audit; row-level files not distributed |
| Cloud registration | Numeric UC version 1; all 862,792 rows compared | Sanitised `reports/cloud_run_acceptance.json` |
| Cloud replay | Two successful writes, 862,792 unique keys each, 0 business-field mismatches | Single-writer assumption; read-only third pass |
| Cloud negative checks | Conflicting manifest and missing alias rejected; table versions unchanged | Same sanitised owner-run report |
| Historical source tests | 23 AML tests + 23 separate ERP regression tests passed | Original local logs retained privately; not 46 tests in this repository |

The public suite contains the 23 AML tests. GitHub Actions provides a separate execution record for this standalone export; passing unit tests does not verify private cloud activity. Design proposals such as production monitoring, concurrent writers or broader data validation are not implied by this table.

The original cloud attempt failed on a Spark Connect write-mode string and was cancelled; the corrected runs passed. See the cloud record. No schedule or paid upgrade was created. Public summaries omit account/billing details and private run identifiers. Exact cloud cost is not established.
