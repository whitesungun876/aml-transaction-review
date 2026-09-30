import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from aml_risk.cloud_contract import target_name,pinned_uri,check_manifest,assert_manifest_reuse,validate_readback
from aml_risk.features import META,FEATURES,SIGNATURE


class CloudContractTests(unittest.TestCase):
    def test_alias_resolved_once_and_pinned(self):
        client=Mock(); client.get_model_version_by_alias.return_value=SimpleNamespace(version='7')
        self.assertEqual(pinned_uri(client,'workspace.default.aml_model','candidate'),('models:/workspace.default.aml_model/7','7'))
        client.get_model_version_by_alias.assert_called_once_with('workspace.default.aml_model','candidate')

    def test_missing_alias_and_bad_version_fail_closed(self):
        client=Mock(); client.get_model_version_by_alias.side_effect=LookupError('missing alias')
        with self.assertRaises(LookupError): pinned_uri(client,'workspace.default.aml_model','candidate')
        client.get_model_version_by_alias.side_effect=None
        for value in ['latest','0','-1','1/../2']:
            client.get_model_version_by_alias.return_value=SimpleNamespace(version=value)
            with self.assertRaises(ValueError): pinned_uri(client,'workspace.default.aml_model','candidate')

    def test_protect_erp_and_sql_identifiers(self):
        for value in ['workspace.default.erp_model','a.b.aml_x;DROP TABLE x','a.b','a.b.c.d','a-b.c.aml_x']:
            with self.assertRaises(ValueError): target_name(value)
        self.assertEqual(target_name('workspace.default.aml_scores'),'workspace.default.aml_scores')

    def test_manifest_conflict_and_label_guard(self):
        m=dict(feature_signature=SIGNATURE,columns=META+FEATURES,batch_id='b1',rows=10,input_sha256='a'*64,model_bundle_version='b'*64,rules_sha256='c'*64)
        identity=check_manifest(m)
        assert_manifest_reuse(identity,identity)
        with self.assertRaises(ValueError): assert_manifest_reuse(identity,check_manifest(m|{'rows':11}))
        with self.assertRaises(ValueError): check_manifest(m|{'columns':META+FEATURES+['label']})
        with self.assertRaises(ValueError): check_manifest(m|{'input_sha256':'unverified'})

    def test_readback_rejects_missing_duplicate_or_changed_scores(self):
        validate_readback(10,10,10,0)
        for values in [(10,9,9,0),(10,11,10,0),(10,10,9,0),(10,10,10,1),(0,0,0,0)]:
            with self.assertRaises(AssertionError): validate_readback(*values)
