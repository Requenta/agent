"""Construct an explicit token-file kubeconfig; never discover host credentials."""
import json
import os
from pathlib import Path
import sys


def main():
    root = Path('/var/run/secrets/kubernetes.io/serviceaccount')
    namespace = os.environ['REQUENTA_KUBE_NAMESPACE']
    for name in ('REQUENTA_ALLOWED_NODES', 'REQUENTA_CLUSTER_REFERENCE', 'REQUENTA_EXECUTOR_ID'):
        if not os.environ.get(name):
            raise ValueError('Missing execution scope')
    if not json.loads(os.environ['REQUENTA_ALLOWED_NODES']):
        raise ValueError('No approved nodes')
    config = {'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'requenta-in-cluster',
              'clusters': [{'name': 'local', 'cluster': {'server': 'https://kubernetes.default.svc', 'certificate-authority': str(root / 'ca.crt')}}],
              'users': [{'name': 'executor', 'user': {'tokenFile': str(root / 'token')}}],
              'contexts': [{'name': 'requenta-in-cluster', 'context': {'cluster': 'local', 'user': 'executor', 'namespace': namespace}}]}
    path = Path('/tmp/kubeconfig')
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    os.environ.update(KUBECONFIG=str(path), REQUENTA_KUBE_CONTEXT='requenta-in-cluster', PYTHONDONTWRITEBYTECODE='1')
    os.execv(sys.executable, [sys.executable, '/app/worker.py', '--origin', os.environ['REQUENTA_ORIGIN'],
                            '--health-file', '/tmp/healthy', '--token-file', '/var/run/requenta-execution/token', '--driver', '/app/workspace_driver.py'])


if __name__ == '__main__':
    main()
