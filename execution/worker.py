"""Offer-scoped execution reconciler. The configured driver owns real provisioning and cleanup.
No simulated GPU, kubeconfig discovery, shell string execution, or live-money capability.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPRedirectHandler
from uuid import uuid4


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward the adapter token to another origin.


class Worker:
    def __init__(self, origin, token_file, driver):
        u = urlparse(origin)
        if u.username or u.password or u.query or u.fragment or u.path not in ('', '/') or not (
            u.scheme == 'https' or (u.scheme == 'http' and u.hostname in ('localhost', '127.0.0.1'))
        ):
            raise ValueError('Use an HTTPS application origin or localhost')
        self.origin = origin.rstrip('/')
        self.token_file = Path(token_file)
        self.driver = Path(driver).resolve(strict=True)
        self.http = build_opener(NoRedirect())
        self.ssh = None
        if os.environ.get('REQUENTA_SSH_ENABLED')=='true':
            from ssh_tunnel import SshManager
            self.ssh = SshManager(self)

    def request(self, path, data=None):
        token = self.token_file.read_text().strip()
        if not token.startswith('rq_exec_test_'):
            raise ValueError('A scoped test adapter token is required')
        body = None if data is None else json.dumps(data,ensure_ascii=False).encode()
        req = Request(self.origin + path, data=body, headers={
            'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
        })
        with self.http.open(req, timeout=15) as response:
            payload = response.read(1048577)
            if len(payload) > 1048576:
                raise ValueError('Oversized response')
            return json.loads(payload) if payload else None

    def emit(self, booking_id, kind, **fields):
        self.request('/api/execution/bookings/' + booking_id + '/events', {
            'key': str(uuid4()), 'kind': kind, **fields,
        })

    def operate(self, operation, task):
        # Explicit executable path; no shell and no buyer-provided command. Credentials stay in the driver config.
        env = {k: v for k, v in os.environ.items() if not k.startswith('STRIPE_') and k not in ('DATABASE_URL', 'DATABASE_MIGRATION_URL')}
        result = subprocess.run([str(self.driver), operation], input=json.dumps(task), text=True,
                                capture_output=True, check=True, timeout=45, env=env)
        if len(result.stdout) > 8192:
            raise ValueError('Oversized driver result')
        data = json.loads(result.stdout)
        if not isinstance(data, dict):
            raise ValueError('Driver must return a JSON object')
        return data

    def reconcile(self, task):
        booking_id, state = task['id'], task['state']
        if self.ssh and (state!='running' or task.get('stop_requested_at')):self.ssh.update(task,[])
        if state in ('completed', 'cancelled', 'failed'):
            result = self.operate('cleanup', task)
            if result.get('deleted') is True:
                self.emit(booking_id, 'cleanup')
            return
        if state == 'running' and task.get('stop_requested_at'):
            result=self.operate('cleanup',task)
            if result.get('deleted') is True:
                self.emit(booking_id,'completed')
                self.emit(booking_id,'cleanup')
            else:self.emit(booking_id,'heartbeat')
            return
        if state == 'reserved':
            # Claim permission before touching hardware. A stale/expired task is rejected server-side.
            self.emit(booking_id, 'provisioning')
        if state in ('reserved', 'provisioning'):
            result = self.operate('ensure', task)
        else:
            result = self.operate('inspect', task)
        phase = result.get('phase')
        if phase == 'failed':
            self.emit(booking_id, 'failed')
        elif state in ('reserved', 'provisioning') and phase == 'ready':
            self.emit(booking_id, 'ready', **({'accessUrl':result['access_url']} if task.get('access_mode')!='workspace' else {}))
        elif state == 'running' and phase in ('ready', 'running'):
            self.emit(booking_id, 'heartbeat')
        elif state == 'running' and phase == 'completed':
            self.emit(booking_id, 'completed')
        elif phase not in ('pending', 'ready', 'running'):
            raise ValueError('Unsupported driver phase or transition')
        if self.ssh and task.get('access_mode')=='workspace' and state in ('ready','running') and phase in ('ready','running') and not task.get('stop_requested_at'):
            try:
                ready=self.operate('ssh-ready',task)
                self.request('/api/execution/bookings/'+booking_id+'/ssh',ready)
                grants=self.request('/api/execution/bookings/'+booking_id+'/ssh')['grants']
                self.ssh.update(task,grants)
            except Exception:
                self.ssh.update(task,[])
        if task.get('access_mode')=='workspace' and state in ('ready','running') and phase in ('ready','running'):
            for command in self.request('/api/execution/bookings/'+booking_id+'/commands')['commands']:
                result=self.operate('command',{**task,'command':command})
                if not result.get('pending'):
                    self.request('/api/execution/bookings/'+booking_id+'/commands',{'commandId':command['id'],'output':result['output'],'exitCode':result['exit_code']})

    def tick(self):
        if os.environ.get('REQUENTA_EXECUTOR_ID'):
            self.operate('reap', {})  # Runs even when the control plane is unreachable.
        response = self.request('/api/execution/tasks')
        if response.get('mode') != 'test':
            raise ValueError('This reconciler only supports the test execution contract')
        healthy = True
        for task in response['tasks']:
            try:
                self.reconcile(task)
            except Exception as exc:
                # Do not expose driver output, tokens or provider credentials in logs.
                healthy = False
                print('Reconciliation will retry:', task.get('id'), type(exc).__name__, flush=True)
        return healthy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--origin', required=True)
    parser.add_argument('--token-file', required=True)
    parser.add_argument('--driver', required=True, help='Absolute path to the operator-approved provisioning executable')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--health-file', type=Path)
    args = parser.parse_args()
    worker = Worker(args.origin, args.token_file, args.driver)
    while True:
        try:
            success = worker.tick()
            if success and args.health_file:
                args.health_file.write_text(str(time.time()))
        except Exception as exc:
            success = False
            print('Control plane unavailable:', type(exc).__name__, flush=True)
        if args.once:
            raise SystemExit(0 if success else 1)
        time.sleep(20)


if __name__ == '__main__':
    main()
