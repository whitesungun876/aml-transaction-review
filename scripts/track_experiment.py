"""Record already-verified runs in local SQLite MLflow; never retrain or upload."""
import argparse
import json
from pathlib import Path
import mlflow
from aml_risk.common import file_sha,write_json

p=argparse.ArgumentParser()
p.add_argument("--experiment",type=Path,required=True)
p.add_argument("--tracking-dir",type=Path,required=True)
a=p.parse_args()
if not (a.experiment/"COMPLETE.json").exists(): raise ValueError("experiment incomplete")
record=a.experiment/"mlflow_runs.json"
if record.exists(): raise FileExistsError("tracked runs already recorded; verify rather than duplicating")
a.tracking_dir.mkdir(parents=True,exist_ok=True)
mlflow.set_tracking_uri("sqlite:///"+str((a.tracking_dir/"mlflow.db").resolve()))
experiment=mlflow.get_experiment_by_name("aml-transaction-review-v1")
eid=experiment.experiment_id if experiment else mlflow.create_experiment("aml-transaction-review-v1",artifact_location=str((a.tracking_dir/"artifacts").resolve()))
reports=json.loads((a.experiment/"validation_results.json").read_text())
final=json.loads((a.experiment/"test_results.json").read_text())
runs=[]
for candidate in reports["candidates"]:
    i=candidate["index"]
    with mlflow.start_run(experiment_id=eid,run_name=f"xgboost-candidate-{i}") as run:
        mlflow.log_params(candidate["manifest"]["provenance"]["params"])
        mlflow.set_tags({"data":"IBM synthetic AML HI-Small release8","selection":"validation-only","model_version":candidate["manifest"]["model_version"],"code_sha256":candidate["manifest"]["provenance"]["code_sha256"]})
        mlflow.log_metrics({"validation_daily_recall_1pct":candidate["validation"]["daily_micro"][0]["recall"],"validation_rank_ap":candidate["validation"]["overall"][0]["average_precision"]})
        if i==final["selected_candidate"]:
            mlflow.log_metrics({"test_daily_recall_1pct":final["xgboost"]["daily_micro"][0]["recall"],"test_rank_ap":final["xgboost"]["overall"][0]["average_precision"]})
        for name in ["model.json","encoding.json","manifest.json"]: mlflow.log_artifact(str(a.experiment/f"candidate-{i}"/name),artifact_path="bundle")
        mlflow.log_artifact(str(a.experiment/"split_manifest.json"))
        runs.append({"type":"xgboost","candidate":i,"run_id":run.info.run_id})
with mlflow.start_run(experiment_id=eid,run_name="rules-baseline") as run:
    mlflow.log_artifact(str(a.experiment/"rules.json"))
    mlflow.log_metrics({"validation_daily_recall_1pct":reports["rules_validation"]["daily_micro"][0]["recall"],"test_daily_recall_1pct":final["rules"]["daily_micro"][0]["recall"]})
    runs.append({"type":"rules","run_id":run.info.run_id})
client=mlflow.MlflowClient()
for item in runs:
    actual=client.get_run(item["run_id"])
    if actual.info.status!="FINISHED" or not actual.data.metrics: raise AssertionError("tracked run readback failed")
write_json(record,{"backend":"local SQLite, not Databricks","runs":runs,"test_report_sha256":file_sha(a.experiment/"test_results.json"),"readback_pass":True})
print(json.dumps({"tracked_runs":len(runs),"readback_pass":True}))
