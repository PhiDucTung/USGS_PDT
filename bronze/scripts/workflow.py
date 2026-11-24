"""Ordered full crawl, validation, export and station preprocessing."""
import os
from pathlib import Path

from .core import API, atomic_json, crawl, export, now, validate
from silver.scripts.preprocess import audit_raw, preprocess


def require_api_key():
    if not os.environ.get('USGS_API_KEY', '').strip():
        raise ValueError('USGS_API_KEY is required before a full crawl. Configure it in the process environment; never put the key in logs or chat.')


def run_all(config, max_donors=20):
    path = Path(config.output) / 'reports' / 'workflow_status.json'
    state = {'started_at': now(), 'status': 'running', 'stage': 'validate_existing',
             'manual_comparison_completed': False}
    atomic_json(path, state)
    try:
        report = validate(config)
        if not report['all_jobs_successful']:
            state.update(stage='crawl', remaining_unplanned_jobs=report['unplanned_jobs'])
            atomic_json(path, state)
            require_api_key()
            api = API(config)
            try:
                crawl(config, api)
            finally:
                api.close()
        state['stage'] = 'validate'
        atomic_json(path, state)
        report = validate(config)
        if not report['all_jobs_successful']:
            raise ValueError('Full crawl has unfinished jobs or validation errors. Rerun after addressing reports/quality.json.')
        state['stage'] = 'audit_raw'
        atomic_json(path, state)
        if not audit_raw(config)['passed']:
            raise ValueError('Raw archive verification failed. See reports/raw_audit.json.')
        state['stage'] = 'export'
        atomic_json(path, state)
        export(config)
        state['stage'] = 'preprocess'
        atomic_json(path, state)
        result = preprocess(config, max_donors)
        state.update(status='complete', stage='complete', completed_at=now(), preprocessing_output=str(result))
        reference = Path(config.output) / 'reports' / 'manual_comparison.json'
        if reference.exists():
            import json
            state['manual_comparison_completed'] = bool(json.loads(reference.read_text()).get('passed'))
        atomic_json(path, state)
        return result
    except BaseException as exc:
        state.update(status='blocked' if isinstance(exc, ValueError) else 'interrupted', stopped_at=now(),
                     reason=str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        atomic_json(path, state)
        raise
