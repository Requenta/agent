#!/usr/bin/env python3
"""Restricted single-node Kubernetes driver for the test execution contract.
Requires an existing dedicated namespace, enforcing CNI, and authenticated supplier gateway.
No cluster discovery, namespace creation, public ingress, host paths, or arbitrary buyer commands.
"""
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
from urllib.parse import urlparse


def required(name):
    value = os.environ.get(name, '')
    if not value:
        raise ValueError('Missing configuration: ' + name)
    return value


def configuration():
    context, namespace = required('REQUENTA_KUBE_CONTEXT'), required('REQUENTA_KUBE_NAMESPACE')
    image, gateway = required('REQUENTA_WORKSPACE_IMAGE'), required('REQUENTA_ACCESS_ORIGIN')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', namespace) or namespace in ('default', 'kube-system', 'kube-public'):
        raise ValueError('A dedicated namespace is required')
    if not re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', image):
        raise ValueError('Pin the operator-approved workspace image by digest')
    u = urlparse(gateway)
    if u.scheme != 'https' or not u.hostname or u.username or u.password or u.query or u.fragment or u.path not in ('', '/'):
        raise ValueError('Configure an existing authenticated HTTPS access gateway origin')
    gateway_namespace = required('REQUENTA_GATEWAY_NAMESPACE')
    gateway_app = required('REQUENTA_GATEWAY_APP')
    for label in (gateway_namespace, gateway_app):
        if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,62}', label):
            raise ValueError('Invalid gateway selector')
    priority = required('REQUENTA_WORKLOAD_PRIORITY_CLASS')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,252}', priority):
        raise ValueError('Invalid workload priority class')
    return context, namespace, image, gateway.rstrip('/'), gateway_namespace, gateway_app, priority


def manifests(task, config, now=None):
    _, namespace, image, _, gateway_ns, gateway_app, priority = config
    now = now or dt.datetime.now(dt.timezone.utc)
    if not re.fullmatch(r'[a-f0-9-]{36}', task['id']):
        raise ValueError('Invalid booking id')
    allocations = task['allocations']
    if len(allocations) != 1 or not 1 <= int(task['gpus']) <= 8:
        raise ValueError('This driver supports one physical node, 1–8 GPUs')
    identity = json.loads(allocations[0]['nodeId'])
    node = identity[2]
    allowed = os.environ.get('REQUENTA_ALLOWED_NODES')
    if allowed is not None and (node not in json.loads(allowed) or identity[1] != required('REQUENTA_CLUSTER_REFERENCE')):
        raise ValueError('Allocation is outside the approved cluster or nodes')
    if not isinstance(node, str) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,252}', node):
        raise ValueError('Invalid selected node name')
    end = dt.datetime.fromisoformat(task['end_at'].replace('Z', '+00:00'))
    ttl = math.floor((end - now).total_seconds())
    if ttl <= 0:
        raise ValueError('The allocation deadline has passed')
    name = 'rq-' + task['id']
    labels = {'requenta.io/booking-id': task['id']}
    executor = os.environ.get('REQUENTA_EXECUTOR_ID')
    if executor:
        labels['requenta.io/executor'] = executor
    metadata = {'name': name, 'namespace': namespace, 'labels': labels,
                'annotations': {'requenta.io/expires-at': end.isoformat()}}
    terms = task.get('resource_terms')
    scratch = '10'
    if terms:
        version = terms.get('version')
        if version not in ('included-v1', 'prepaid-transfer-v1', 'resource-bundle-v2'):
            raise ValueError('Unsupported resource contract')
        scratch = terms.get('scratchGiB')
        if not isinstance(scratch, str) or not re.fullmatch(r'[1-9][0-9]{0,3}', scratch):
            raise ValueError('Invalid disk capacity')
        if version != 'resource-bundle-v2' and scratch != '10':
            raise ValueError('Historical disk capacity is fixed')
        if version == 'resource-bundle-v2' and os.environ.get('REQUENTA_RESOURCE_BUNDLE_ENABLED') != 'true':
            raise ValueError('Resource bundle execution is not enabled')
        if int(scratch) > min(2000, int(os.environ.get('REQUENTA_MAX_SCRATCH_GIB', '2000'))):
            raise ValueError('Disk exceeds qualified capacity')
    gpus = int(task['gpus'])
    resources = {'nvidia.com/gpu': str(gpus), 'cpu': str(gpus * 4), 'memory': str(gpus * 16) + 'Gi', 'ephemeral-storage': str(int(scratch) + 10) + 'Gi'}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': metadata, 'spec': {
        'automountServiceAccountToken': False, 'enableServiceLinks': False,
        'activeDeadlineSeconds': ttl, 'restartPolicy': 'Never',
        'priorityClassName': priority, 'preemptionPolicy': 'Never',
        'nodeSelector': {'kubernetes.io/hostname': node},
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000, 'runAsGroup': 1000, 'fsGroup': 1000, 'seccompProfile': {'type': 'RuntimeDefault'}},
        'containers': [{'name': 'workspace', 'image': image, 'imagePullPolicy': 'IfNotPresent',
            'ports': [{'containerPort': 8888}],
            'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
            'resources': {'requests': resources, 'limits': resources},
            'readinessProbe': {'tcpSocket': {'port': 8888}, 'initialDelaySeconds': 3, 'periodSeconds': 5},
            'volumeMounts': [{'name': 'scratch', 'mountPath': '/workspace'}, {'name': 'tmp', 'mountPath': '/tmp'}]}],
        'volumes': [{'name': 'scratch', 'emptyDir': {'sizeLimit': scratch + 'Gi'}}, {'name': 'tmp', 'emptyDir': {'sizeLimit': '1Gi'}}],
    }}
    service = {'apiVersion': 'v1', 'kind': 'Service', 'metadata': metadata, 'spec': {
        'type': 'ClusterIP', 'selector': labels, 'ports': [{'port': 8888, 'targetPort': 8888}],
    }}
    policy = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': metadata, 'spec': {
        'podSelector': {'matchLabels': labels}, 'policyTypes': ['Ingress', 'Egress'],
        'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': gateway_ns}},
                              'podSelector': {'matchLabels': {'app': gateway_app}}}], 'ports': [{'port': 8888, 'protocol': 'TCP'}]}],
        'egress': [],
    }}
    return [policy, service, pod]


class Kubernetes:
    def __init__(self, config):
        self.config = config

    def command(self, args, payload=None):
        result = subprocess.run(['kubectl', '--context', self.config[0], '--namespace', self.config[1], *args],
                                input=None if payload is None else json.dumps(payload), capture_output=True, text=True, timeout=12, check=True)
        if args[0]=='delete':return None
        return json.loads(result.stdout) if result.stdout.strip() else None

    def read_owned(self, kind, task):
        result = self.command(['get', kind, 'rq-' + task['id'], '--ignore-not-found', '-o', 'json'])
        if result and (result.get('metadata', {}).get('labels', {}).get('requenta.io/booking-id') != task['id']
                       or (os.environ.get('REQUENTA_EXECUTOR_ID') and result.get('metadata', {}).get('labels', {}).get('requenta.io/executor') != os.environ['REQUENTA_EXECUTOR_ID'])):
            raise ValueError('Existing resource is not owned by this booking')
        return result

    def inspect(self, task):
        pod = self.read_owned('pod', task)
        if not pod:
            return {'phase': 'pending'}
        phase = pod.get('status', {}).get('phase')
        if phase in ('Failed', 'Succeeded'):
            return {'phase': 'completed' if phase == 'Succeeded' else 'failed'}
        ready = any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in pod.get('status', {}).get('conditions', []))
        return {'phase': 'ready' if ready else 'pending', 'access_url': self.config[3] + '/sessions/' + task['id']}

    def reap(self):
        executor = required('REQUENTA_EXECUTOR_ID')
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', executor):
            raise ValueError('Invalid executor identity')
        pods = self.command(['get', 'pods', '-l', 'requenta.io/executor=' + executor, '-o', 'json'])
        for pod in pods.get('items', []):
            meta = pod['metadata']
            booking = meta.get('labels', {}).get('requenta.io/booking-id', '')
            if not re.fullmatch(r'[a-f0-9-]{36}', booking) or meta['name'] != 'rq-' + booking:
                continue
            expiry = dt.datetime.fromisoformat(meta['annotations']['requenta.io/expires-at'])
            if expiry <= dt.datetime.now(dt.timezone.utc) or pod.get('status', {}).get('phase') in ('Failed', 'Succeeded'):
                # Direct cleanup does not assert a control-plane outcome; the next task poll does that.
                Kubernetes.run(self, 'cleanup', {'id': booking})
        return {'reaped': True}

    def run(self, operation, task):
        if operation == 'reap':
            return self.reap()
        if operation == 'ensure':
            # Policies precede the workload. Existing pods are inspected, never replaced/restarted on retry.
            resources = manifests(task, self.config)
            for resource in resources:
                if not self.read_owned(resource['kind'].lower(), task):
                    self.command(['create', '-f', '-', '-o', 'json'], resource)
            return self.inspect(task)
        if operation == 'inspect':
            return self.inspect(task)
        if operation == 'cleanup':
            # Keep the deny policy until the pod is actually gone, including finalizers/termination.
            if self.read_owned('pod', task):
                self.command(['delete', 'pod', 'rq-' + task['id'], '--wait=false', '-o', 'name'])
                if self.read_owned('pod', task):
                    return {'deleted': False}
            for kind in ('service', 'networkpolicy'):
                if self.read_owned(kind, task):
                    self.command(['delete', kind, 'rq-' + task['id'], '--wait=false', '-o', 'name'])
            deleted = all(self.read_owned(kind, task) is None for kind in ('pod', 'service', 'networkpolicy'))
            return {'deleted': deleted}
        raise ValueError('Unsupported operation')


def main():
    try:
        task = json.load(sys.stdin)
        result = Kubernetes(configuration()).run(sys.argv[1], task)
        print(json.dumps(result))
    except Exception as exc:
        # Never print kubeconfig, subprocess stderr, cluster endpoints or tokens.
        print(type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
