"""Synthetic unit fixtures test the gate only, not the Databricks integration."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('accept_cloud', Path(__file__).resolve().parents[1] / 'scripts/accept_cloud.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.states = {}
        for phase in ('register', 'first', 'repeat', 'verify'):
            result = dict(action=phase if phase in ('register', 'verify') else 'score',
                          manifest_identity='input', model_bundle_version='bundle',
                          model_name='model', model_version='1', checked_rows=10,
                          input_rows=10, output_rows=10, unique_keys=10,
                          business_mismatches=0, review_queue_rows=8629,
                          table='workspace.default.aml_transaction_scores_v1',
                          negative_checks=dict(persisted_ledger_conflict_rejected=True,
                                               missing_uc_alias_rejected_without_fallback=True,
                                               table_versions_unchanged=True))
            self.states[phase] = dict(run_id=phase, state={'result_state': 'SUCCESS'},
                                      tasks=[{'state': {'result_state': 'SUCCESS'}}],
                                      notebook_output={'result': json.dumps(result), 'truncated': False})
        self.manifest = {'rows': 10, 'model_bundle_version': 'bundle'}

    def test_matching_evidence(self):
        self.assertTrue(gate.reconcile(self.states, self.manifest, 'input')['real_cloud_execution_verified'])

    def test_reject_failed_and_truncated(self):
        for field in ('failed', 'truncated'):
            states = copy.deepcopy(self.states)
            if field == 'failed': states['repeat']['state']['result_state'] = 'FAILED'
            else: states['verify']['notebook_output']['truncated'] = True
            with self.assertRaises(AssertionError): gate.reconcile(states, self.manifest, 'input')

    def test_reject_wrong_version_counts_and_missing_guards(self):
        for key, value in [('model_version', '2'), ('output_rows', 11), ('business_mismatches', 1), ('negative_checks', {})]:
            states = copy.deepcopy(self.states)
            result = json.loads(states['verify']['notebook_output']['result'])
            result[key] = value
            states['verify']['notebook_output']['result'] = json.dumps(result)
            with self.assertRaises(AssertionError): gate.reconcile(states, self.manifest, 'input')
