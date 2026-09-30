# Databricks notebook source
"""Prepared T7 runner. NOT executed or cloud-accepted by local tests.

Requires explicitly approved resources/costs and a manually uploaded package.
No schedule, no training, no label upload. One writer and one active run only.
"""
import json
import math
import re
import sys
from pathlib import Path

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def main(spark, dbutils):
    dbutils.widgets.text('config', '')
    config=json.loads(dbutils.widgets.get('config'))
    if config.get('cloud_write_approved') is not True:
        raise ValueError('explicit cloud write/cost approval is required')
    package=Path(config['package_dir'])
    if not re.fullmatch(r'/Volumes/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*/aml_[A-Za-z0-9_]+/[A-Za-z0-9_-]+',str(package)):
        raise ValueError('package must be in a dedicated AML volume directory')
    # Only code from the locally prepared, approved package is loaded.
    # Verify its hash against the explicitly supplied package identity first.
    import hashlib
    manifest_path=package/'manifest.json'
    actual_manifest_sha=hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if actual_manifest_sha!=config['manifest_sha256']:
        raise ValueError('uploaded manifest differs from approved package')
    manifest=json.loads(manifest_path.read_text())
    for relative,expected in manifest['model_files'].items():
        path=package/'model'/relative
        if path.is_symlink() or not path.resolve().is_relative_to((package/'model').resolve()):
            raise ValueError('unsafe artifact path')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
            raise ValueError('uploaded model package checksum mismatch')
    sys.path.insert(0,str(package/'model/code'))
    from aml_risk.common import file_sha,digest
    from aml_risk.features import META,FEATURES,NUMERIC,SIGNATURE
    from aml_risk.model import RULE_FEATURES
    from aml_risk.cloud_contract import target_name,pinned_uri,check_manifest,assert_manifest_reuse,validate_readback
    identity=check_manifest(manifest)
    if file_sha(package/'input.parquet')!=manifest['input_sha256']:
        raise ValueError('uploaded input checksum mismatch')
    rules=json.loads((package/'rules.json').read_text())
    if digest(rules)!=manifest['rules_sha256']: raise ValueError('rules changed')
    model_name=target_name(config['model_name'])
    output_table=target_name(config['output_table'])
    ledger_table=target_name(config['ledger_table'])
    if output_table==ledger_table: raise ValueError('scores and ledger must be separate')
    alias=config['alias']
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]*',alias): raise ValueError('unsafe alias')
    mlflow.set_tracking_uri('databricks')
    mlflow.set_registry_uri('databricks-uc')
    client=MlflowClient(registry_uri='databricks-uc')
    owner='aml-transaction-review-v1'

    if config['action']=='register':
        experiment=config['experiment_path']
        if not experiment.startswith('/Shared/aml-') or '..' in experiment:
            raise ValueError('experiment must be in dedicated /Shared/aml- namespace')
        prior=mlflow.get_experiment_by_name(experiment)
        if prior is not None and prior.tags.get('aml_owner')!=owner:
            raise ValueError('refusing an unowned existing experiment')
        eid=prior.experiment_id if prior is not None else mlflow.create_experiment(experiment,tags={'aml_owner':owner})
        try:
            existing=client.get_registered_model(model_name)
        except MlflowException as exc:
            if exc.error_code!='RESOURCE_DOES_NOT_EXIST': raise
            client.create_registered_model(model_name,tags={'aml_owner':owner})
        else:
            # Registration is deliberately one-shot. An interrupted run needs
            # read-only inspection, not blind repeated version/alias creation.
            raise ValueError('registered model already exists; inspect before retry')
        local=mlflow.pyfunc.load_model(str(package/'model'))
        from mlflow.models import Model
        descriptor=Model.load(str(package/'model/MLmodel'))
        data=descriptor.flavors['python_function']['data']
        with mlflow.start_run(experiment_id=eid,run_name='aml-frozen-model-registration') as run:
            logged=mlflow.pyfunc.log_model(name='aml_ranker',loader_module='aml_risk.mlflow_loader',data_path=str(package/'model'/data),code_paths=[str(package/'model/code/aml_risk')],signature=descriptor.signature,pip_requirements=str(package/'model/requirements.txt'),metadata=descriptor.metadata)
            version=str(mlflow.register_model(logged.model_uri,model_name).version)
            pinned=mlflow.pyfunc.load_model(f'models:/{model_name}/{version}')
            # Full input comparison in bounded batches, not only five examples.
            import pyarrow.parquet as pq
            count=0
            for batch in pq.ParquetFile(package/'input.parquet').iter_batches(batch_size=25000):
                frame=batch.to_pandas()[FEATURES].astype({x:'float64' for x in NUMERIC})
                np.testing.assert_allclose(pinned.predict(frame)['score'],local.predict(frame)['score'],atol=1e-6,rtol=0)
                count+=len(frame)
            if count!=manifest['rows']: raise AssertionError('registration row count mismatch')
            client.set_model_version_tag(model_name,version,'model_bundle_version',manifest['model_bundle_version'])
            client.set_registered_model_alias(model_name,alias,version)
            uri,resolved=pinned_uri(client,model_name,alias)
            if resolved!=version: raise AssertionError('alias readback mismatch')
            return {'action':'register','run_id':run.info.run_id,'model_name':model_name,'model_version':version,'model_bundle_version':manifest['model_bundle_version'],'checked_rows':count,'manifest_identity':identity,'registered_uri':uri}

    if config['action'] not in ['score','verify']: raise ValueError('unknown action')
    read_only=config['action']=='verify'
    before_versions={}
    if read_only:
        for table in [ledger_table,output_table]:
            before_versions[table]=DeltaTable.forName(spark,table).history(1).select('version').first().version
    uri,version=pinned_uri(client,model_name,alias)  # once per batch
    model=mlflow.pyfunc.load_model(uri)
    if model.metadata.metadata.get('model_bundle_version')!=manifest['model_bundle_version']:
        raise ValueError('resolved alias is not the frozen model bundle')
    frame=pd.read_parquet(package/'input.parquet')
    if list(frame.columns)!=META+FEATURES or len(frame)!=manifest['rows'] or frame.transaction_id.isna().any() or frame.transaction_id.duplicated().any():
        raise ValueError('input schema, labels or identities invalid')
    if frame.event_time.dt.tz is not None: raise ValueError('unknown-timezone contract changed')
    chunks=[]
    for start in range(0,len(frame),25000):
        inputs=frame.iloc[start:start+25000][FEATURES].astype({x:'float64' for x in NUMERIC})
        chunks.append(model.predict(inputs)['score'].to_numpy())
    values=np.concatenate(chunks)
    if not np.isfinite(values).all(): raise ValueError('nonfinite scores')
    result=frame[META].copy()
    # Preserve unspecified source time explicitly as text, not UTC conversion.
    result['event_time']=frame.event_time.dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    result['score']=values.astype('float64')
    result['batch_id']=manifest['batch_id']; result['model_id']=model_name
    result['model_version']=version; result['model_bundle_version']=manifest['model_bundle_version']
    result['feature_version']=digest(SIGNATURE); result['manifest_identity']=identity
    result['score_type']='raw_margin_not_calibrated_probability'
    result['explanation_type']='training_rule_context_not_model_attribution'
    result['reason_codes']=[json.dumps([f'above_train_q99:{name}' for name,value in zip(RULE_FEATURES,row) if value>rules[name]],separators=(',',':')) for row in frame[RULE_FEATURES].itertuples(index=False,name=None)]
    result=result.sort_values(['score','transaction_id'],ascending=[False,True],kind='stable')
    result['rank']=np.arange(1,len(result)+1,dtype=np.int64)
    result['review_date']=result.event_time.str[:10]
    result['daily_rank']=result.groupby('review_date',sort=False).cumcount()+1
    result['daily_review_budget']=np.ceil(result.groupby('review_date').transaction_id.transform('count')*.01).astype('int64')
    source=spark.createDataFrame(result)
    if not read_only:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ledger_table} (batch_id STRING, manifest_identity STRING) USING DELTA TBLPROPERTIES ('aml_owner'='{owner}')")
    for table in [ledger_table,output_table]:
        if spark.catalog.tableExists(table):
            properties={row.key:row.value for row in spark.sql(f'SHOW TBLPROPERTIES {table}').collect()}
            if properties.get('aml_owner')!=owner: raise ValueError('refusing to write an unowned table')
    ledger=spark.table(ledger_table).where(F.col('batch_id')==manifest['batch_id']).collect()
    if len(ledger)>1: raise AssertionError('duplicate batch ledger keys')
    if ledger: assert_manifest_reuse(ledger[0].manifest_identity,identity)
    else:
        if read_only: raise AssertionError('verification requires an existing batch ledger')
        spark.createDataFrame([(manifest['batch_id'],identity)],'batch_id string, manifest_identity string').write.mode('append').saveAsTable(ledger_table)
    if not spark.catalog.tableExists(output_table):
        if read_only: raise AssertionError('verification requires existing scores')
        source.limit(0).write.format('delta').mode('error').option('aml_owner',owner).saveAsTable(output_table)
        spark.sql(f"ALTER TABLE {output_table} SET TBLPROPERTIES ('aml_owner'='{owner}')")
    predicate=(F.col('batch_id')==manifest['batch_id']) & (F.col('model_id')==model_name) & (F.col('model_version')==version)
    # Never update existing business scores. A conflicting replay is an error.
    old=spark.table(output_table).where(predicate).select(source.columns)
    if old.exceptAll(source).limit(1).count(): raise ValueError('stored scores differ; refusing overwrite')
    if not read_only:
        (DeltaTable.forName(spark,output_table).alias('t').merge(source.alias('s'),'t.batch_id=s.batch_id AND t.model_id=s.model_id AND t.model_version=s.model_version AND t.transaction_id=s.transaction_id').whenNotMatchedInsertAll().execute())
    actual=spark.table(output_table).where(predicate).select(source.columns)
    mismatches=actual.exceptAll(source).limit(1).count()+source.exceptAll(actual).limit(1).count()
    count=actual.count(); unique=actual.select('batch_id','model_id','model_version','transaction_id').distinct().count()
    validate_readback(len(frame),count,unique,mismatches)
    queue=actual.where(F.col('daily_rank')<=F.col('daily_review_budget')).count()
    negative_checks={}
    if read_only:
        conflicting=dict(manifest,rows=manifest['rows']+1)
        try:
            assert_manifest_reuse(ledger[0].manifest_identity,check_manifest(conflicting))
        except ValueError as exc:
            if 'batch identity conflict' not in str(exc): raise
            negative_checks['persisted_ledger_conflict_rejected']=True
        else: raise AssertionError('changed manifest accepted')
        try:
            pinned_uri(client,model_name,'missing_t7_'+identity[:16])
        except MlflowException as exc:
            if exc.error_code!='RESOURCE_DOES_NOT_EXIST': raise
            negative_checks['missing_uc_alias_rejected_without_fallback']=True
        else: raise AssertionError('test alias unexpectedly exists')
        after_versions={table:DeltaTable.forName(spark,table).history(1).select('version').first().version for table in before_versions}
        if before_versions!=after_versions: raise AssertionError('tables changed during read-only verification')
        negative_checks['table_versions_unchanged']=True
        negative_checks['table_versions']=after_versions
    return {'action':config['action'],'input_rows':len(frame),'output_rows':count,'unique_keys':unique,'business_mismatches':mismatches,'review_queue_rows':queue,'model_name':model_name,'model_version':version,'model_bundle_version':manifest['model_bundle_version'],'manifest_identity':identity,'table':output_table,'single_writer':True,'negative_checks':negative_checks}


if __name__=='__main__':
    dbutils.notebook.exit(json.dumps(main(spark,dbutils),sort_keys=True))
