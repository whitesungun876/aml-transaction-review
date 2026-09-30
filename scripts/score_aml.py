import argparse
import json
from pathlib import Path
import duckdb
from aml_risk.batch import publish
from aml_risk.common import file_sha,write_json
from aml_risk.features import META,FEATURES,SIGNATURE

p=argparse.ArgumentParser()
p.add_argument("--features",type=Path,required=True)
p.add_argument("--experiment",type=Path,required=True)
p.add_argument("--output",type=Path,required=True)
p.add_argument("--batch-id",required=True)
a=p.parse_args()
selection=json.loads((a.experiment/"selection.json").read_text())
split=json.loads((a.experiment/"split_manifest.json").read_text())
if file_sha(a.features)!=split["features_sha256"]: raise ValueError("frozen feature snapshot changed")
bounds=split["splits"]["test"]
con=duckdb.connect(); con.from_parquet(str(a.features)).create_view("f")
frame=con.execute("SELECT "+",".join(META+FEATURES)+" FROM f WHERE event_time>=? AND event_time<? ORDER BY event_time,transaction_id",[bounds["start"],bounds["end_exclusive"]]).fetchdf()
identity={"batch_id":a.batch_id,"features_sha256":file_sha(a.features),"test_bounds":bounds,"feature_signature":SIGNATURE}
rules=json.loads((a.experiment/"rules.json").read_text())
model=a.experiment/f"candidate-{selection['selected_candidate']}"
first=publish(frame,model,identity,a.output,rules)
second=publish(frame,model,identity,a.output,rules)
changed=frame.copy(); changed.loc[changed.index[0],"log_payment_amount"]+=1
try: publish(changed,model,identity,a.output,rules)
except ValueError as exc:
    if "identity conflict" not in str(exc): raise
    conflict=True
else: raise AssertionError("changed batch was not rejected")
report={"first":first,"replay":second,"changed_input_conflict_rejected":conflict}
write_json(a.output/"acceptance.json",report)
print(json.dumps(report))
