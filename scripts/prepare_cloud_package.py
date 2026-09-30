"""Build and verify a label-free, full-test cloud package locally. No network."""
import argparse
import json
import shutil
from pathlib import Path
import duckdb
import mlflow.pyfunc
from mlflow.models import ModelSignature
from mlflow.types import Schema,ColSpec
import numpy as np
import pandas as pd
from aml_risk.common import file_sha,write_json,digest
from aml_risk.features import META,FEATURES,NUMERIC,CATEGORICAL,SIGNATURE
from aml_risk.model import Ranker
from aml_risk.cloud_contract import check_manifest


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.output.exists(): raise FileExistsError('cloud package exists; do not overwrite')
    root=Path('.runtime'); experiment=root/'experiment-v1'
    split=json.loads((experiment/'split_manifest.json').read_text())
    features=root/'prepared-v1/features.parquet'
    if file_sha(features)!=split['features_sha256']: raise ValueError('frozen features changed')
    selection=json.loads((experiment/'selection.json').read_text())
    bundle=experiment/f"candidate-{selection['selected_candidate']}"
    ranker=Ranker.load(bundle)
    model_manifest=json.loads((bundle/'manifest.json').read_text())
    a.output.mkdir(parents=True)
    con=duckdb.connect(); con.execute('SET threads=2'); con.execute("SET memory_limit='2GB'")
    con.from_parquet(str(features)).create_view('f')
    bounds=split['splits']['test']
    frame=con.execute('SELECT '+','.join(META+FEATURES)+' FROM f WHERE event_time>=? AND event_time<? ORDER BY event_time,transaction_id',[bounds['start'],bounds['end_exclusive']]).fetchdf()
    con.close()
    if len(frame)!=bounds['rows'] or frame.transaction_id.duplicated().any(): raise AssertionError('cloud package input mismatch')
    frame.to_parquet(a.output/'input.parquet',index=False)
    shutil.copyfile(experiment/'rules.json',a.output/'rules.json')
    signature=ModelSignature(inputs=Schema([ColSpec('double',x) for x in NUMERIC]+[ColSpec('string',x) for x in CATEGORICAL]),outputs=Schema([ColSpec('float','score')]))
    requirements=['mlflow==3.16.1','xgboost-cpu==3.4.1','scikit-learn==1.9.0','numpy==2.5.3','pandas==2.2.3','pyarrow==19.0.1','scipy==1.18.1','duckdb==1.3.2']
    mlflow.pyfunc.save_model(path=str(a.output/'model'),loader_module='aml_risk.mlflow_loader',data_path=str(bundle),code_paths=['src/aml_risk'],signature=signature,pip_requirements=requirements,metadata={'model_bundle_version':model_manifest['model_version'],'feature_version':digest(SIGNATURE),'score_type':'raw_margin_not_calibrated_probability'})
    loaded=mlflow.pyfunc.load_model(str(a.output/'model'))
    acceptance=json.loads((root/'batches/acceptance.json').read_text())
    scores_path=Path(acceptance['first']['path'])/'scores.parquet'
    scores=pd.read_parquet(scores_path,columns=['transaction_id','score']).set_index('transaction_id').score
    max_error=0.0
    for start in range(0,len(frame),25000):
        part=frame.iloc[start:start+25000]
        inputs=part[FEATURES].astype({x:'float64' for x in NUMERIC})
        actual=loaded.predict(inputs)['score'].to_numpy()
        expected=scores.loc[part.transaction_id].to_numpy()
        np.testing.assert_allclose(actual,expected,atol=1e-6,rtol=0)
        max_error=max(max_error,float(np.max(np.abs(actual-expected))))
    unknown=frame.iloc[:5][FEATURES].astype({x:'float64' for x in NUMERIC})
    unknown[CATEGORICAL]='__UNSEEN_CATEGORY__'
    np.testing.assert_allclose(loaded.predict(unknown)['score'],ranker.score(unknown),atol=1e-6,rtol=0)
    manifest={'batch_id':'hi-small-test-v1','rows':len(frame),'columns':META+FEATURES,'feature_signature':SIGNATURE,
        'input_sha256':file_sha(a.output/'input.parquet'),'rules_sha256':digest(json.loads((a.output/'rules.json').read_text())),
        'model_bundle_version':model_manifest['model_version'],'frozen_feature_sha256':file_sha(features),
        'split_sha256':file_sha(experiment/'split_manifest.json'),'scope':'full frozen test, no labels',
        'model_files':{str(p.relative_to(a.output/'model')):file_sha(p) for p in sorted((a.output/'model').rglob('*')) if p.is_file() and '__pycache__' not in p.parts}}
    check_manifest(manifest)
    write_json(a.output/'manifest.json',manifest)
    report={'status':'local_preflight_only_not_cloud_acceptance','rows':len(frame),'max_abs_score_error':max_error,'score_atol':1e-6,'unknown_category_equivalence':True,'manifest_sha256':file_sha(a.output/'manifest.json'),'manifest_identity':digest(manifest),'package':str(a.output),'cloud_executed':False}
    write_json(a.output/'COMPLETE.json',report)
    write_json(Path('reports/cloud_package_preflight.json'),report)
    print(json.dumps(report))


if __name__=='__main__': main()
