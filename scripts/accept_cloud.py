"""Validate saved real Databricks results; never accept mocks as cloud evidence."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def reconcile(states, manifest, identity):
    results = {}
    for phase in ('register', 'first', 'repeat', 'verify'):
        state = states[phase]
        if state['state'].get('result_state') != 'SUCCESS':
            raise AssertionError(f'{phase}: job not successful')
        if not state['tasks'] or any(t['state'].get('result_state') != 'SUCCESS' for t in state['tasks']):
            raise AssertionError(f'{phase}: task not successful')
        output = state['notebook_output']
        if output.get('truncated') or state.get('error'):
            raise AssertionError(f'{phase}: incomplete notebook evidence')
        result = json.loads(output['result'])
        if result['manifest_identity'] != identity or result['model_bundle_version'] != manifest['model_bundle_version']:
            raise AssertionError(f'{phase}: changed input or model bundle')
        results[phase] = result
    registered = results['register']
    if registered['action'] != 'register' or registered['checked_rows'] != manifest['rows']:
        raise AssertionError('incomplete registered-model comparison')
    for phase in ('first', 'repeat', 'verify'):
        result = results[phase]
        expected_action = 'verify' if phase == 'verify' else 'score'
        if result['action'] != expected_action:
            raise AssertionError('wrong action')
        for key in ('model_name', 'model_version'):
            if result[key] != registered[key]:
                raise AssertionError('numeric model version changed')
        if any(result[key] != manifest['rows'] for key in ('input_rows', 'output_rows', 'unique_keys')):
            raise AssertionError('row/key reconciliation failed')
        if result['business_mismatches'] != 0 or result['review_queue_rows'] != 8629:
            raise AssertionError('business result reconciliation failed')
        if result['table'] != 'workspace.default.aml_transaction_scores_v1':
            raise AssertionError('unexpected output table')
    checks = results['verify']['negative_checks']
    for key in ('persisted_ledger_conflict_rejected', 'missing_uc_alias_rejected_without_fallback', 'table_versions_unchanged'):
        if checks.get(key) is not True:
            raise AssertionError('negative/read-only check missing')
    return {'real_cloud_execution_verified': True,
            'job_run_ids': {phase: states[phase]['run_id'] for phase in states},
            'results': results}


def main():
    state_dir = ROOT / '.runtime/cloud-execution'
    states = {phase: json.loads((state_dir / f'{phase}_status.json').read_text())
              for phase in ('register', 'first', 'repeat', 'verify')}
    manifest_path = ROOT / '.runtime/cloud-v1/manifest.json'
    manifest = json.loads(manifest_path.read_text())
    complete = json.loads((manifest_path.parent / 'COMPLETE.json').read_text())
    if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != complete['manifest_sha256']:
        raise AssertionError('local manifest changed')
    report = reconcile(states, manifest, complete['manifest_identity'])
    closeout_path = ROOT / 'reports/cloud_closeout.json'
    closeout = json.loads(closeout_path.read_text()) if closeout_path.exists() else {}
    closed = (closeout.get('job_run_ids') == report['job_run_ids']
              and closeout.get('no_active_runs') is True
              and closeout.get('active_run_check', {}).get('result') == []
              and closeout.get('final_trial_ui_observed') is True
              and closeout.get('payment_method_added_or_paid_upgrade_performed') is False)
    report['t7_accepted'] = closed
    report['closeout_evidence'] = 'reports/cloud_closeout.json' if closed else None
    report['remaining_acceptance_checks'] = [] if closed else ['confirm no active AML runs and record final trial balance observation']
    path = ROOT / 'reports/cloud_run_acceptance.json'
    path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'cloud_execution_checks': 'PASS', 'report': str(path)}))


if __name__ == '__main__':
    main()
