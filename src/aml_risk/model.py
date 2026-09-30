"""Training-only encoding bundled with a JSON XGBoost model; no pickle loading."""
import json
from pathlib import Path
import numpy as np
from scipy import sparse
from sklearn.preprocessing import OneHotEncoder
import xgboost as xgb
from .features import FEATURES, NUMERIC, CATEGORICAL, SIGNATURE
from .common import file_sha, write_json, digest


def validate(frame):
    if list(frame.columns) != FEATURES:
        raise ValueError("feature signature mismatch, unexpected columns or label leakage")
    if not len(frame) or frame.isna().any().any():
        raise ValueError("empty or missing feature values")
    if not np.isfinite(frame[NUMERIC].to_numpy(dtype=np.float64)).all():
        raise ValueError("nonfinite numeric features")


class Ranker:
    def __init__(self, encoder, booster):
        self.encoder, self.booster = encoder, booster

    def matrix(self, frame):
        validate(frame)
        return sparse.hstack([sparse.csr_matrix(frame[NUMERIC].to_numpy(dtype=np.float32)), self.encoder.transform(frame[CATEGORICAL])], format="csr", dtype=np.float32)

    @classmethod
    def fit(cls, frame, labels, params, seed):
        validate(frame)
        labels = np.asarray(labels)
        if len(labels) != len(frame) or set(np.unique(labels)) != {0, 1}:
            raise ValueError("training needs aligned binary labels of both classes")
        encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=True, dtype=np.float32)
        encoder.fit(frame[CATEGORICAL])
        model = cls(encoder, None)
        estimator = xgb.XGBClassifier(**params, tree_method="hist", n_jobs=2, random_state=seed, eval_metric="logloss")
        estimator.fit(model.matrix(frame), labels)
        model.booster = estimator.get_booster()
        return model

    def score(self, frame):
        result = self.booster.predict(xgb.DMatrix(self.matrix(frame)), output_margin=True)
        if not np.isfinite(result).all(): raise ValueError("nonfinite model scores")
        return result

    def save(self, path, provenance):
        path=Path(path)
        if path.exists(): raise FileExistsError("model bundle exists")
        path.mkdir(parents=True)
        self.booster.save_model(path/"model.json")
        encoding={"categories":[list(map(str,c)) for c in self.encoder.categories_], "signature":SIGNATURE}
        write_json(path/"encoding.json",encoding)
        manifest={"model_sha256":file_sha(path/"model.json"),"encoding_sha256":file_sha(path/"encoding.json"),"provenance":provenance,"score_type":"raw_margin_not_calibrated_probability"}
        manifest["model_version"]=digest(manifest)
        write_json(path/"manifest.json",manifest)
        return manifest

    @classmethod
    def load(cls,path):
        path=Path(path); manifest=json.loads((path/"manifest.json").read_text())
        version=manifest.pop("model_version")
        if digest(manifest)!=version: raise ValueError("model manifest identity mismatch")
        if file_sha(path/"model.json")!=manifest["model_sha256"] or file_sha(path/"encoding.json")!=manifest["encoding_sha256"]:
            raise ValueError("model artifact checksum mismatch")
        encoding=json.loads((path/"encoding.json").read_text())
        if encoding["signature"]!=SIGNATURE: raise ValueError("feature signature mismatch")
        encoder=OneHotEncoder(categories=encoding["categories"],handle_unknown="ignore",sparse_output=True,dtype=np.float32)
        import pandas as pd
        encoder.fit(pd.DataFrame([[c[0] for c in encoding["categories"]]],columns=CATEGORICAL))
        booster=xgb.Booster(); booster.load_model(path/"model.json")
        if booster.num_features()!=len(NUMERIC)+sum(map(len,encoding["categories"])):
            raise ValueError("encoded feature count mismatch")
        return cls(encoder,booster)


RULE_FEATURES=["send_count_24h","counterparties_24h","amount_to_history_ratio"]


def fit_rules(frame):
    validate(frame)
    return {name:float(frame[name].quantile(0.99)) for name in RULE_FEATURES}


def score_rules(frame, thresholds):
    validate(frame)
    return sum((frame[name].to_numpy()>thresholds[name]).astype(float) for name in RULE_FEATURES)
