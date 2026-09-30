"""Read-only model audit; fresh scoring to a separate root, no model selection."""
import json
from pathlib import Path
import duckdb
import numpy as np
from aml_risk.batch import publish
from aml_risk.common import file_sha,write_json
from aml_risk.features import META,FEATURES

root=Path('.runtime'); experiment=root/'experiment-v1'
accept=json.loads((root/'batches/acceptance.json').read_text())
published=Path(accept['first']['path'])
manifest=json.loads((published/'run_manifest.json').read_text())
con=duckdb.connect(); con.execute("SET threads=2"); con.execute("SET memory_limit='2GB'")
con.from_parquet(str(root/'prepared-v1/features.parquet')).create_view('features')
con.from_parquet(str(published/'scores.parquet')).create_view('scores')
con.from_parquet(str(root/'prepared-v1/labels.parquet')).create_view('labels')
con.execute('CREATE VIEW audit AS SELECT s.*,l.label,f.payment_format,f.payment_currency FROM scores s JOIN labels l USING(transaction_id) JOIN features f USING(transaction_id)')
counts=con.sql('SELECT count(*),count(DISTINCT transaction_id),sum(label),sum(CASE WHEN daily_rank<=daily_review_budget THEN 1 ELSE 0 END),sum(CASE WHEN daily_rank<=daily_review_budget THEN label ELSE 0 END) FROM audit').fetchone()
assert counts==(862792,862792,956,8629,603),counts
missing=con.sql('SELECT count(*) FROM scores s LEFT JOIN features f USING(transaction_id) WHERE f.transaction_id IS NULL OR s.source_file_sha256<>f.source_file_sha256 OR s.source_record_number<>f.source_record_number OR s.dataset_version<>f.dataset_version').fetchone()[0]
assert missing==0
frame=con.sql('SELECT '+','.join('f.'+x for x in META+FEATURES)+' FROM features f JOIN scores s USING(transaction_id) ORDER BY f.event_time,f.transaction_id').fetchdf()
selection=json.loads((experiment/'selection.json').read_text())
fresh=publish(frame,experiment/f"candidate-{selection['selected_candidate']}",manifest['batch']['identity'],root/'independent-score-replay',manifest['rule_thresholds'])
con.from_parquet(str(Path(fresh['path'])/'scores.parquet')).create_view('fresh')
differences=con.sql('(SELECT * FROM scores EXCEPT ALL SELECT * FROM fresh) UNION ALL (SELECT * FROM fresh EXCEPT ALL SELECT * FROM scores)').fetchall()
assert not differences,'fresh scoring differs'
slices={}
for field in ['review_date','payment_format','payment_currency']:
    cursor=con.sql(f'SELECT {field},count(*) AS rows,sum(label) AS positives,sum(CASE WHEN daily_rank<=daily_review_budget THEN 1 ELSE 0 END) AS reviewed,sum(CASE WHEN daily_rank<=daily_review_budget THEN label ELSE 0 END) AS hits FROM audit GROUP BY {field} ORDER BY {field}')
    slices[field]=cursor.fetchdf().to_dict('records')
cases={}
for name,condition in [('false_positive','label=0 AND daily_rank<=daily_review_budget'),('false_negative','label=1 AND daily_rank>daily_review_budget')]:
    cases[name]=con.sql(f'SELECT transaction_id,source_file_sha256,source_record_number,score,reason_codes,review_date FROM audit WHERE {condition} ORDER BY rank LIMIT 5').fetchdf().to_dict('records')
write_json(root/'private_failure_examples.json',cases)
report={'counts':dict(zip(['rows','unique_ids','positives','reviewed','hits'],counts)),
        'source_reference_mismatches':missing,'fresh_scoring_exact_match':True,
        'scores_sha256':file_sha(published/'scores.parquet'),'slices':slices,
        'failure_examples':'local only: .runtime/private_failure_examples.json',
        'scope':'frozen test, post-hoc diagnostic only; no retuning'}
write_json(Path('reports/output_audit.json'),report)
print(json.dumps({k:v for k,v in report.items() if k!='slices'}))
