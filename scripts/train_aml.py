"""Freeze protocol first; choose on validation, then evaluate untouched test once."""
import argparse
import gc
import json
import platform
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import xgboost
import sklearn
from aml_risk.common import write_json,file_sha,digest
from aml_risk.features import FEATURES,SIGNATURE
from aml_risk.protocol import freeze
from aml_risk.model import Ranker,fit_rules,score_rules
from aml_risk.evaluation import evaluate

p=argparse.ArgumentParser()
p.add_argument("--prepared",type=Path,required=True)
p.add_argument("--output",type=Path,required=True)
p.add_argument("--config",type=Path,default=Path("configs/aml_experiment.json"))
a=p.parse_args()
if a.output.exists(): raise FileExistsError("experiment output already exists; do not overwrite final test")
if not (a.prepared/"COMPLETE.json").exists(): raise ValueError("adapter not accepted")
accepted=json.loads((a.prepared/"COMPLETE.json").read_text())
for name,key in [("transactions.parquet","transactions_sha256"),("labels.parquet","labels_sha256"),("quality_report.json","quality_sha256")]:
    if file_sha(a.prepared/name)!=accepted[key]: raise ValueError("adapter artifact integrity failed")
feature_manifest=json.loads((a.prepared/"features.manifest.json").read_text())
if feature_manifest["signature"]!=SIGNATURE or file_sha(a.prepared/"features.parquet")!=feature_manifest["output_sha256"] or feature_manifest["input_sha256"]!=accepted["transactions_sha256"]: raise ValueError("feature artifact integrity failed")
a.output.mkdir(parents=True)
config=json.loads(a.config.read_text())
splits=freeze(a.prepared/"features.parquet",config,a.output/"split_manifest.json")
codefiles=sorted(Path("src/aml_risk").glob("*.py"))+[Path("scripts/train_aml.py"),Path("src/erp_risk/evaluation.py")]
code_hash=digest({str(f):file_sha(f) for f in codefiles})
con=duckdb.connect(); con.execute("SET memory_limit='2GB'"); con.execute("SET threads=2")
con.from_parquet(str(a.prepared/"features.parquet")).create_view("f")
con.from_parquet(str(a.prepared/"labels.parquet")).create_view("l")
if con.execute("SELECT count(*) FROM l").fetchone()[0]!=con.execute("SELECT count(DISTINCT transaction_id) FROM l").fetchone()[0]: raise ValueError("duplicate label keys")
con.execute("CREATE VIEW data AS SELECT f.*,l.label FROM f JOIN l USING(transaction_id)")
if con.execute("SELECT count(*) FROM data").fetchone()[0]!=con.execute("SELECT count(*) FROM f").fetchone()[0]: raise ValueError("labels do not cover features")

def load(name):
    bounds=splits["splits"][name]
    return con.execute("SELECT transaction_id,event_time,"+",".join(FEATURES)+",label FROM data WHERE event_time>=? AND event_time<? ORDER BY event_time,transaction_id",[bounds["start"],bounds["end_exclusive"]]).fetchdf()

support={name:dict(zip(["rows","positives"],con.execute("SELECT count(*),sum(label) FROM data WHERE event_time>=? AND event_time<?",[b["start"],b["end_exclusive"]]).fetchone())) for name,b in splits["splits"].items()}
write_json(a.output/"support.json",support)
if any(support[n]["positives"]<config["minimum_positive_support"] for n in ["train","validation","test"]): raise ValueError("insufficient positive support; no model fit")
train=load("train"); validation=load("validation")
rules=fit_rules(train[FEATURES]); write_json(a.output/"rules.json",rules)
reports={"rules_validation":evaluate(validation,score_rules(validation[FEATURES],rules),validation.label),"candidates":[]}
for index,params in enumerate(config["models"]):
    ranker=Ranker.fit(train[FEATURES],train.label,params,config["seed"])
    scores=ranker.score(validation[FEATURES])
    report=evaluate(validation,scores,validation.label)
    manifest=ranker.save(a.output/f"candidate-{index}",{"params":params,"seed":config["seed"],"code_sha256":code_hash,"split_sha256":file_sha(a.output/"split_manifest.json"),"python":platform.python_version(),"xgboost":xgboost.__version__,"sklearn":sklearn.__version__})
    loaded=Ranker.load(a.output/f"candidate-{index}")
    np.testing.assert_allclose(scores,loaded.score(validation[FEATURES]),rtol=0,atol=1e-6)
    reports["candidates"].append({"index":index,"validation":report,"manifest":manifest,"roundtrip_pass":True})
    write_json(a.output/"validation_results.json",reports)
    print(json.dumps({"candidate":index,"validation_daily_1pct":report["daily_micro"][0]}),flush=True)
    del ranker,loaded; gc.collect()
best=max(reports["candidates"],key=lambda r:r["validation"]["daily_micro"][0]["recall"])
write_json(a.output/"selection.json",{"selected_candidate":best["index"],"metric":config["selection_metric"],"validation_only":True})
del train,validation; gc.collect()
test=load("test"); ranker=Ranker.load(a.output/f"candidate-{best['index']}")
final={"selected_candidate":best["index"],"xgboost":evaluate(test,ranker.score(test[FEATURES]),test.label),"rules":evaluate(test,score_rules(test[FEATURES],rules),test.label),"limitations":["synthetic ecosystem, not single-bank visibility","transaction labels are not independent case labels","primary interval only; generator tail excluded before fitting","two held-out calendar days do not establish population generalisation"]}
write_json(a.output/"test_results.json",final)
write_json(a.output/"COMPLETE.json",{"test_report_sha256":file_sha(a.output/"test_results.json"),"code_sha256":code_hash})
print(json.dumps({"test":{k:final[k]["daily_micro"] for k in ["rules","xgboost"]}}),flush=True)
