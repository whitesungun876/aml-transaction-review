"""Pure validation shared by offline tests and the pending Databricks runner."""
import re
from .common import digest
from .features import META, FEATURES, SIGNATURE


def target_name(value):
    parts = value.split('.')
    if len(parts) != 3 or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', p) for p in parts):
        raise ValueError('expected safe catalog.schema.object')
    if not parts[2].startswith('aml_'):
        raise ValueError('cloud resources must use the separate aml_ namespace')
    return value


def pinned_uri(client, model_name, alias):
    target_name(model_name)
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]*', alias):
        raise ValueError('unsafe alias')
    # No fallback to latest or an unversioned model when the alias is absent.
    resolved = client.get_model_version_by_alias(model_name, alias)
    version = str(resolved.version)
    if not re.fullmatch(r'[1-9][0-9]*', version):
        raise ValueError('alias did not resolve to a numeric version')
    return f'models:/{model_name}/{version}', version


def check_manifest(manifest):
    if manifest['feature_signature'] != SIGNATURE or manifest['columns'] != META + FEATURES:
        raise ValueError('cloud input signature mismatch or unexpected label')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', manifest['batch_id']):
        raise ValueError('unsafe batch identity')
    if type(manifest['rows']) is not int or manifest['rows'] < 1:
        raise ValueError('empty or invalid cloud batch')
    for name in ['input_sha256', 'model_bundle_version', 'rules_sha256']:
        if not re.fullmatch(r'[a-f0-9]{64}', manifest[name]):
            raise ValueError('missing content identity')
    return digest(manifest)


def assert_manifest_reuse(existing, intended):
    if existing != intended:
        raise ValueError('batch identity conflict; do not overwrite the ledger')


def validate_readback(input_rows, output_rows, unique_keys, mismatches):
    if input_rows <= 0 or (input_rows, input_rows, 0) != (output_rows, unique_keys, mismatches):
        raise AssertionError('cloud output reconciliation failed')
