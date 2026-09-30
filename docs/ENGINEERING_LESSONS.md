# Reusable engineering lessons

Use these patterns when adapting an existing model pipeline to a new domain. Change dataset fields, time bounds, budgets and cloud names explicitly; keep the safeguards.

## 1. Reuse contracts, not domain assumptions

Reuse a small budget evaluator; rebuild transaction identity, labels and temporal features for the new source. Verify the exact CSV header, including duplicate names, before positional parsing. A source-hash/record-number identity is a provenance key, not a business ID.

## 2. Freeze before fitting

Record source hash, observation interval, split boundaries, candidate list, seed and selection metric first. Fit encoding on training only. Test that adding same-time or future events cannot change a row's historical features. Once test failures are inspected, treat proposed improvements as a new protocol needing a new holdout.

## 3. Translate model quality into review workload

Report selected rows, hits, total positives, precision and recall together, using per-day rounding when capacity is daily. Include tie bounds and slices: an attractive overall recall can hide a complete coverage gap in another payment channel. Raw margins and rule reason codes are not probabilities or causal explanations.

## 4. Make replay verifiable

Bind batch identity to the manifest and numeric model version. Reject an existing identity with changed content; reconcile complete fields, not row counts alone. Separate mutable alias resolution from pinned batch execution. State the single-writer assumption; idempotent replay alone does not prove concurrent safety.

## 5. Treat failed deployment as evidence

After failure, inspect job and table state before retrying. Retain the failure cause, cancelled retries and recovery checks. Separate local package equivalence, successful real cloud execution and public synthetic CI. A billing screenshot or timeout is not a cost guarantee.

Publication checklist: explicit file allowlist; no datasets, per-row results, model binaries, credentials or personal materials; portable dependencies; clean standalone tests; honest historical-vs-current evidence; source and licence attribution.
