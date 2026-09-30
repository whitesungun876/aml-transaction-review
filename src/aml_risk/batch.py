"""Immutable local batch publication, checksum verification and conflict checks."""
import json
import re
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from .common import digest,file_sha,write_json
from .features import FEATURES,META,SIGNATURE
from .model import Ranker,RULE_FEATURES


def publish(frame, model_dir, identity, output_root, rules):
    if list(frame.columns)!=META+FEATURES or frame.transaction_id.duplicated().any() or not len(frame):
        raise ValueError("batch schema, labels or duplicate IDs invalid")
    batch_id=identity["batch_id"]
    if not re.fullmatch(r"[a-zA-Z0-9_-]+",batch_id): raise ValueError("unsafe batch ID")
    if identity.get("feature_signature")!=SIGNATURE: raise ValueError("batch feature signature mismatch")
    model_dir=Path(model_dir); output_root=Path(output_root)
    model_manifest=json.loads((model_dir/"manifest.json").read_text())
    ranker=Ranker.load(model_dir)  # integrity check even on idempotent replay
    # Bind exact content as well as caller manifest, so same IDs with changed
    # values cannot silently pass. Hash schema + pandas content deterministically.
    import hashlib
    content=hashlib.sha256(pd.util.hash_pandas_object(frame,index=False).values.tobytes()).hexdigest()
    batch_manifest={"identity":identity,"content_sha256":content,"rows":len(frame),"columns":list(frame.columns),"rule_context_sha256":digest(rules)}
    output_root.mkdir(parents=True,exist_ok=True)
    batch_root=output_root/batch_id
    batch_root.mkdir(exist_ok=True)
    identity_path=batch_root/"identity.json"
    if identity_path.exists():
        if json.loads(identity_path.read_text())!=batch_manifest: raise ValueError("batch identity conflict")
    else:
        # Exclusive creation, first writer only. No claim of distributed locking.
        try:
            with identity_path.open("x") as stream: json.dump(batch_manifest,stream,sort_keys=True)
        except FileExistsError:
            raise RuntimeError("concurrent batch initialization; retry after checking writer")
    destination=batch_root/model_manifest["model_version"]
    if destination.exists():
        complete=json.loads((destination/"COMPLETE.json").read_text())
        for name,sha in complete["files"].items():
            if file_sha(destination/name)!=sha: raise ValueError("published artifact checksum mismatch")
        return {"status":"verified_replay","rows":complete["rows"],"path":str(destination)}
    with tempfile.TemporaryDirectory(prefix=".batch-",dir=batch_root) as temp:
        temp=Path(temp)
        scores=ranker.score(frame[FEATURES])
        result=frame[META].copy()
        result["score"]=scores
        result["model_id"]="aml-xgboost"
        result["model_version"]=model_manifest["model_version"]
        result["feature_version"]=digest(SIGNATURE)
        result["batch_id"]=batch_id
        result["score_type"]="raw_margin_not_calibrated_probability"
        # Explanations are explicit rule context, not false model explanations.
        contexts=[]
        for values in frame[RULE_FEATURES].itertuples(index=False,name=None):
            contexts.append(json.dumps([f"above_train_q99:{name}" for name,value in zip(RULE_FEATURES,values) if value>rules[name]],separators=(",",":")))
        result["reason_codes"]=contexts
        result["explanation_type"]="training_rule_context_not_model_attribution"
        result=result.sort_values(["score","transaction_id"],ascending=[False,True],kind="stable")
        result["rank"]=np.arange(1,len(result)+1)
        result["review_date"]=result.event_time.dt.date.astype(str)
        result["daily_rank"]=result.groupby("review_date",sort=False).cumcount()+1
        result["daily_review_budget"]=np.ceil(result.groupby("review_date").transaction_id.transform("count")*0.01).astype(int)
        result.to_parquet(temp/"scores.parquet",index=False)
        queue=result[result.daily_rank<=result.daily_review_budget]
        queue.to_parquet(temp/"review_queue.parquet",index=False)
        if result.transaction_id.nunique()!=len(frame): raise ValueError("output reconciliation failed")
        write_json(temp/"run_manifest.json",{"batch":batch_manifest,"model":model_manifest,"rule_thresholds":rules,"explanation_limit":"Rules contextualise inputs, not XGBoost causal reasons","numpy":np.__version__,"pandas":pd.__version__})
        files={name:file_sha(temp/name) for name in ["scores.parquet","review_queue.parquet","run_manifest.json"]}
        write_json(temp/"COMPLETE.json",{"rows":len(frame),"review_queue_rows":len(queue),"files":files})
        temp.rename(destination)
    return {"status":"published","rows":len(frame),"review_queue_rows":len(queue),"path":str(destination)}
