import sys
import os
import yaml


def _transform_op(op):
    """Transform a single operation dict in-place:
    - response example: → examples: mock: value:
    - x-microcks-operation dispatcherRules → return "mock"
    """
    if not isinstance(op, dict):
        return

    # Only touch response content, not requestBody content.
    for _code, resp in op.get('responses', {}).items():
        if not isinstance(resp, dict):
            continue
        for _media, media_obj in resp.get('content', {}).items():
            if not isinstance(media_obj, dict):
                continue

            if 'example' in media_obj and 'examples' not in media_obj:
                # Anonymous example → named 'mock'
                media_obj['examples'] = {'mock': {'value': media_obj.pop('example')}}
            elif 'examples' in media_obj and 'mock' not in media_obj['examples']:
                # Already named but no 'mock' key → add alias from first entry
                first = next(iter(media_obj['examples'].values()), {'value': {}})
                media_obj['examples']['mock'] = first
            elif 'examples' not in media_obj and 'example' not in media_obj:
                # No example at all → add empty mock so dispatcher can resolve
                media_obj['examples'] = {'mock': {'value': {}}}

    # Simplify inline dispatcherRules to just return the 'mock' example name.
    # Use Groovy single-quote string to keep the YAML scalar free of double quotes.
    microcks = op.get('x-microcks-operation')
    if isinstance(microcks, dict) and microcks.get('dispatcher') == 'SCRIPT':
        if not microcks.get('dispatcherRules'):
            microcks['dispatcherRules'] = "return 'mock'"


def _ensure_health_endpoint(data):
    """Add /health GET endpoint if missing.

    The Consul health check registered by spotify.groovy / tmdb.groovy
    polls  GET /rest/<Service>/<Version>/health  every 10 s.  If that
    path is absent from the YAML, Microcks returns 404, Consul marks the
    service critical, and after DeregisterCriticalServiceAfter (30 s) it
    removes the service from the registry entirely.
    """
    paths = data.setdefault('paths', {})
    if '/health' in paths:
        return
    paths['/health'] = {
        'get': {
            'operationId': 'healthCheck',
            'summary': 'Health check',
            'responses': {
                '200': {
                    'description': 'OK',
                    'content': {
                        'application/json': {
                            'examples': {
                                'mock': {'value': {'status': 'ok'}}
                            }
                        }
                    }
                }
            }
        }
    }


def transform_yaml(data):
    _ensure_health_endpoint(data)

    paths = data.get('paths', {})
    if not isinstance(paths, dict):
        return
    for _path, path_obj in paths.items():
        if not isinstance(path_obj, dict):
            continue
        for method, op in path_obj.items():
            if method in ('parameters', 'summary', 'description', 'servers', '$ref'):
                continue
            _transform_op(op)


def fix_yaml_dir(apis_dir):
    for fname in sorted(os.listdir(apis_dir)):
        if not fname.endswith(('.yaml', '.yml')):
            continue
        fpath = os.path.join(apis_dir, fname)
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            if data is None:
                print(f'[WARN] {fname} is empty, skipping')
                continue

            transform_yaml(data)

            with open(fpath, 'w', encoding='utf-8') as f:
                yaml.dump(
                    data, f,
                    allow_unicode=True,
                    sort_keys=False,
                    default_flow_style=False,
                    width=100000,
                )
            paths_count = len(data.get('paths', {}))
            print(f'[OK] {fname} — {paths_count} paths transformed')
        except yaml.YAMLError as e:
            print(f'[ERROR] {fname}: {e}')


if __name__ == '__main__':
    apis_dir = sys.argv[1] if len(sys.argv) > 1 else '/apis'
    fix_yaml_dir(apis_dir)
