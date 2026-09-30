"""Run in an isolated, network-disabled container without the workspace source."""
import argparse
import hashlib
import json
from pathlib import Path
import mlflow.pyfunc
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--package',type=Path,required=True)
    p.add_argument('--expected-scores',type=Path,required=True)
    p.add_argument('--report',type=Path,required=True)
    a=p.parse_args()
    manifest=json.loads((a.package/'manifest.json').read_text())
    if sha(a.package/'input.parquet')!=manifest['input_sha256']: raise ValueError('input changed')
    for relative,expected in manifest['model_files'].items():
        if sha(a.package/'model'/relative)!=expected: raise ValueError('model package changed')
    model=mlflow.pyfunc.load_model(str(a.package/'model'))
    import aml_risk.model as bundled_module
    if not Path(bundled_module.__file__).resolve().is_relative_to((a.package/'model/code').resolve()):
        raise AssertionError('loaded workspace module instead of packaged module')
    from aml_risk.features import FEATURES,NUMERIC
    expected=pd.read_parquet(a.expected_scores,columns=['transaction_id','score']).set_index('transaction_id').score
    if expected.index.has_duplicates: raise AssertionError('duplicate expected identities')
    rows=0; max_error=0.0; seen=set()
    for batch in pq.ParquetFile(a.package/'input.parquet').iter_batches(batch_size=25000):
        frame=batch.to_pandas()
        ids=frame.transaction_id.tolist()
        if len(set(ids))!=len(ids) or seen.intersection(ids): raise AssertionError('duplicate input identities')
        seen.update(ids)
        actual=model.predict(frame[FEATURES].astype({x:'float64' for x in NUMERIC}))['score'].to_numpy()
        reference=expected.loc[ids].to_numpy()
        np.testing.assert_allclose(actual,reference,atol=1e-6,rtol=0)
        max_error=max(max_error,float(np.max(np.abs(actual-reference))))
        rows+=len(frame)
    if rows!=manifest['rows'] or len(expected)!=rows: raise AssertionError('row count mismatch')
    report={'rows':rows,'max_abs_score_error':max_error,'score_atol':1e-6,'module_loaded_from_package':True,'manifest_sha256':sha(a.package/'manifest.json'),'status':'isolated_local_model_load_pass_not_cloud_acceptance'}
    a.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__=='__main__': main()
