#!/usr/bin/env python3
"""Outbound adapter workspace access. Buyer code executes only inside the allocated pod.
No public endpoint, host shell interpolation, host mount or buyer Kubernetes credentials.
"""
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from kubernetes_driver import Kubernetes, configuration, manifests, required
from egress_proxy import rule

PROBE = "import json,torch;print(json.dumps({'cuda':torch.cuda.is_available(),'gpus':torch.cuda.device_count(),'models':[torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())]}))"
RUNNER = r'''
import json,os,signal,subprocess,sys,threading,time
from pathlib import Path
p=Path(sys.argv[1]);d=json.loads((p/'input.json').read_text())
argv=['python','-I','-c',d['code']] if d['kind'] in ('python','probe') else ['/bin/sh','-lc',d['code']]
output=bytearray()
def read(pipe):
 while True:
  chunk=pipe.read(1024)
  if not chunk:break
  if len(output)<3000:output.extend(chunk[:3000-len(output)])
try:
 child=subprocess.Popen(argv,cwd='/workspace',stdout=subprocess.PIPE,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
 t=threading.Thread(target=read,args=(child.stdout,),daemon=True);t.start()
 try:code=child.wait(timeout=d['timeout'])
 except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();code=124
 t.join(timeout=2)
 # A command may leave background descendants holding stdout open; stop that process group too.
 try:os.killpg(child.pid,signal.SIGKILL)
 except ProcessLookupError:pass
 text=output.decode('utf8',errors='replace');text=''.join(c if ord(c)>=32 or c in '\n\t' else '?' for c in text)
 text=text.encode('utf8')[:3000].decode('utf8',errors='ignore')
 result={'output':text,'exit_code':code if 0<=code<=255 else 1}
except Exception as e:result={'output':type(e).__name__,'exit_code':1}
(p/'result.tmp').write_text(json.dumps(result,ensure_ascii=False));os.replace(p/'result.tmp',p/'result.json')
'''
DISPATCH = r'''
import json,os,subprocess,sys,time
from pathlib import Path
r=json.load(sys.stdin);p=Path('/tmp/requenta-commands')/r['id'];p.parent.mkdir(exist_ok=True)
try:
 p.mkdir();(p/'input.json').write_text(json.dumps(r))
 subprocess.Popen(['python','-I','-c',r['runner'],str(p)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
except FileExistsError:pass
if (p/'result.json').exists():print((p/'result.json').read_text())
elif time.time()-p.stat().st_mtime>r['timeout']+30:print(json.dumps({'output':'Command outcome unavailable; not retried','exit_code':125}))
else:print(json.dumps({'pending':True}))
'''

SSH_PREPARE = r'''
import json,os,subprocess,sys
from pathlib import Path
root=Path('/tmp/requenta-ssh');root.mkdir(mode=0o700,exist_ok=True)
Path('/workspace/home').mkdir(mode=0o700,exist_ok=True)
if not (root/'host_key').exists():
 subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(root/'host_key')],check=True)
request=json.load(sys.stdin)
if request.get('public_key'):
 directory=Path('/workspace/home/.ssh');directory.mkdir(mode=0o700,exist_ok=True)
 p=directory/'authorized_keys';p.write_text(request['public_key']+'\n');p.chmod(0o600)
subprocess.run(['/usr/sbin/sshd','-t','-f','/etc/requenta/sshd_config'],check=True)
print(json.dumps({'hostKey':' '.join((root/'host_key.pub').read_text().split()[:2])}))
'''

def workspace_config():
    # Legacy gateway fields are unused for this outbound, command-based workspace driver.
    os.environ.setdefault('REQUENTA_ACCESS_ORIGIN','https://unused.invalid')
    os.environ.setdefault('REQUENTA_GATEWAY_NAMESPACE','requenta-unused')
    os.environ.setdefault('REQUENTA_GATEWAY_APP','unused')
    return configuration()


PRIVATE_V4=['10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','169.254.0.0/16','100.64.0.0/10','127.0.0.0/8','0.0.0.0/8']


def egress_rules(task):
    """RFC-022: the booking's frozen host rules, or None. Refused unless the operator enabled the proxy."""
    e=task.get('egress')
    if not e:return None
    if os.environ.get('REQUENTA_EGRESS_PROXY_ENABLED')!='true':raise ValueError('Egress proxy is not enabled')
    if e.get('version')!='egress-proxy-v1' or not isinstance(e.get('rules'),list) or not 1<=len(e['rules'])<=64:raise ValueError('Unsupported egress contract')
    return [rule(r) for r in e['rules']]


def egress_manifests(task, config, rules, token=None, now=None):
    """Secret, policy and pod for one booking's proxy. No Service (the workload quota allows none): the workload
    reaches the pod IP. Egress is DNS plus public addresses on 443; the proxy itself refuses non-allow-listed hosts."""
    _,namespace,_,_,_,_,priority=config
    _,_,workload=manifests(task,config,now)
    image=required('REQUENTA_EGRESS_PROXY_IMAGE')
    name='rq-egress-'+task['id']
    labels={'requenta.io/egress-proxy':task['id']}
    if os.environ.get('REQUENTA_EXECUTOR_ID'):labels['requenta.io/executor']=os.environ['REQUENTA_EXECUTOR_ID']
    metadata={'name':name,'namespace':namespace,'labels':labels,'annotations':workload['metadata']['annotations']}
    control=[{'ipBlock':{'cidr':c}} for c in json.loads(os.environ.get('REQUENTA_CONTROL_PLANE_CIDRS','[]'))]
    policy={'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':metadata,'spec':{
        'podSelector':{'matchLabels':labels},'policyTypes':['Ingress','Egress'],
        'ingress':[{'from':[{'podSelector':{'matchLabels':{'requenta.io/booking-id':task['id']}}}],'ports':[{'port':3128,'protocol':'TCP'}]}],
        'egress':[{'to':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'kube-system'}},'podSelector':{'matchLabels':{'k8s-app':'kube-dns'}}}],
                   'ports':[{'port':53,'protocol':'UDP'},{'port':53,'protocol':'TCP'}]},
                  {'to':[{'ipBlock':{'cidr':'0.0.0.0/0','except':PRIVATE_V4}},*control],'ports':[{'port':443,'protocol':'TCP'}]}]}}
    limits={'cpu':'500m','memory':'256Mi','ephemeral-storage':'64Mi'}
    pod={'apiVersion':'v1','kind':'Pod','metadata':metadata,'spec':{
        'automountServiceAccountToken':False,'enableServiceLinks':False,'restartPolicy':'Always',
        'activeDeadlineSeconds':workload['spec']['activeDeadlineSeconds'],
        'priorityClassName':priority,'preemptionPolicy':'Never','nodeSelector':workload['spec']['nodeSelector'],
        'securityContext':{'runAsNonRoot':True,'runAsUser':65532,'runAsGroup':65532,'fsGroup':65532,'seccompProfile':{'type':'RuntimeDefault'}},
        'containers':[{'name':'egress','image':image,'imagePullPolicy':'IfNotPresent','command':['python3','/app/egress_proxy.py','--port','3128'],
            'env':[{'name':'REQUENTA_EGRESS_ALLOW','value':','.join(rules)},{'name':'REQUENTA_EGRESS_METER','value':'http'},
                   {'name':'REQUENTA_ORIGIN','value':required('REQUENTA_ORIGIN')},{'name':'REQUENTA_EGRESS_TOKEN_FILE','value':'/var/run/requenta-egress/token'},
                   {'name':'PYTHONDONTWRITEBYTECODE','value':'1'},{'name':'PYTHONUNBUFFERED','value':'1'}],
            'ports':[{'containerPort':3128}],'readinessProbe':{'tcpSocket':{'port':3128},'periodSeconds':2},
            'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
            'resources':{'requests':{'cpu':'100m','memory':'128Mi','ephemeral-storage':'64Mi'},'limits':limits},
            'volumeMounts':[{'name':'credential','mountPath':'/var/run/requenta-egress','readOnly':True}]}],
        'volumes':[{'name':'credential','secret':{'secretName':name,'defaultMode':0o440}}]}}
    secret=None
    if token is not None:
        if not re.fullmatch(r'rq_egress_[a-f0-9]{64}',token):raise ValueError('Invalid egress credential')
        secret={'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':namespace,'labels':labels},'type':'Opaque','stringData':{'token':token}}
    return secret,policy,pod


def workspace_manifests(task, config, now=None, proxy_ip=None):
    policy,_,pod=manifests(task,config,now)
    policy['spec']['ingress']=[]
    container=pod['spec']['containers'][0]
    container.pop('ports');container.pop('readinessProbe')
    container['command']=['python','-I','-c','import time; time.sleep(2147483647)']
    container['env']=[{'name':'HOME','value':'/tmp'},{'name':'PYTHONDONTWRITEBYTECODE','value':'1'},{'name':'NVIDIA_DRIVER_CAPABILITIES','value':'compute,utility'}]
    if proxy_ip:
        # Only the booking's proxy is reachable, by IP, so the workload needs neither DNS nor any other route.
        policy['spec']['egress']=[{'to':[{'podSelector':{'matchLabels':{'requenta.io/egress-proxy':task['id']}}}],'ports':[{'port':3128,'protocol':'TCP'}]}]
        url='http://'+proxy_ip+':3128'
        container['env']+=[{'name':n,'value':url} for n in ('HTTPS_PROXY','HTTP_PROXY','https_proxy','http_proxy')]+[{'name':n,'value':'localhost,127.0.0.1'} for n in ('NO_PROXY','no_proxy')]
    container['readinessProbe']={'exec':{'command':['python','-I','-c',f'import torch; assert torch.cuda.is_available() and torch.cuda.device_count()=={int(task["gpus"])}']},'initialDelaySeconds':3,'periodSeconds':15,'timeoutSeconds':10,'failureThreshold':2}
    cap=os.environ.get('REQUENTA_WORKSPACE_MAX_LIFETIME_SECONDS')
    if cap:
        if not cap.isdigit() or not 60<=int(cap)<=86400:raise ValueError('Invalid local safety lifetime')
        pod['spec']['activeDeadlineSeconds']=min(pod['spec']['activeDeadlineSeconds'],int(cap))
    return [policy,pod]


class Workspace(Kubernetes):
    def read_egress(self, kind, task):
        result=self.command(['get',kind,'rq-egress-'+task['id'],'--ignore-not-found','-o','json'])
        labels=(result or {}).get('metadata',{}).get('labels',{})
        if result and (labels.get('requenta.io/egress-proxy')!=task['id'] or (os.environ.get('REQUENTA_EXECUTOR_ID') and labels.get('requenta.io/executor')!=os.environ['REQUENTA_EXECUTOR_ID'])):
            raise ValueError('Existing resource is not owned by this booking')
        return result

    def ensure_proxy(self, task, rules):
        """Returns the proxy pod IP once ready, None while starting, or a request for a credential. The credential is
        issued once per proxy: an existing Secret is never replaced, so a restarted worker does not rotate it."""
        if not self.read_egress('secret',task):
            if not task.get('egress_token'):return {'phase':'pending','egress_token_required':True}
            secret,_,_=egress_manifests(task,self.config,rules,task['egress_token'])
            self.command(['create','-f','-','-o','name'],secret)
        _,policy,pod=egress_manifests(task,self.config,rules)
        for resource in (policy,pod):
            if not self.read_egress(resource['kind'].lower(),task):self.command(['create','-f','-','-o','json'],resource)
        current=self.read_egress('pod',task) or {}
        ready=any(c.get('type')=='Ready' and c.get('status')=='True' for c in current.get('status',{}).get('conditions',[]))
        return current.get('status',{}).get('podIP') if ready else None

    def cleanup_egress(self, task):
        if self.read_egress('pod',task):
            self.command(['delete','pod','rq-egress-'+task['id'],'--ignore-not-found','--wait=false','-o','name'])
            if self.read_egress('pod',task):return False
        for kind in ('networkpolicy','secret'):
            if self.read_egress(kind,task):self.command(['delete',kind,'rq-egress-'+task['id'],'--ignore-not-found','--wait=false','-o','name'])
        return all(self.read_egress(kind,task) is None for kind in ('pod','networkpolicy','secret'))

    def reap(self):
        result=super().reap()
        executor=required('REQUENTA_EXECUTOR_ID')
        # Proxies are found through their policy, which is deleted last, so an interrupted cleanup resumes next pass.
        for policy in (self.command(['get','networkpolicies','-l','requenta.io/executor='+executor+',requenta.io/egress-proxy','-o','json']) or {}).get('items',[]):
            meta=policy['metadata'];booking=meta.get('labels',{}).get('requenta.io/egress-proxy','')
            if not re.fullmatch(r'[a-f0-9-]{36}',booking) or meta['name']!='rq-egress-'+booking:continue
            if dt.datetime.fromisoformat(meta.get('annotations',{}).get('requenta.io/expires-at','2999-01-01T00:00:00+00:00'))<=dt.datetime.now(dt.timezone.utc):
                self.cleanup_egress({'id':booking})
        return result

    def run(self, operation, task):
        if operation=='reap':return self.reap()
        if task.get('access_mode')!='workspace':raise ValueError('Wrong access driver')
        if operation=='ensure':
            rules=egress_rules(task)
            proxy_ip=None
            if rules:
                proxy_ip=self.ensure_proxy(task,rules)
                if isinstance(proxy_ip,dict):return proxy_ip
                if not proxy_ip:return {'phase':'pending'}
            for resource in workspace_manifests(task,self.config,proxy_ip=proxy_ip):
                if not self.read_owned(resource['kind'].lower(),task):self.command(['create','-f','-','-o','json'],resource)
            return self.inspect(task)
        if operation=='cleanup':
            result=super().run('cleanup',task)
            # The workload goes first; its proxy and credential follow. Absent egress objects make this a no-op.
            return {'deleted':result.get('deleted') is True and self.cleanup_egress(task)}
        if operation in ('ssh-ready','ssh-spec'):
            pod=self.read_owned('pod',task)
            if not pod or pod.get('status',{}).get('phase')!='Running':raise ValueError('Workspace is not running')
            if os.environ.get('REQUENTA_SSH_ENABLED')!='true':raise ValueError('SSH disabled')
            request={}
            deadline=dt.datetime.fromisoformat(task['end_at'].replace('Z','+00:00'))
            if operation=='ssh-spec':
                grant=task['ssh_grant']
                if not re.fullmatch(r'ssh-ed25519 [A-Za-z0-9+/]{68}',grant['public_key']):raise ValueError('Invalid public key')
                deadline=min(deadline,dt.datetime.fromisoformat(grant['expires_at'].replace('Z','+00:00')))
                request={'public_key':grant['public_key']}
            remaining=int((deadline-dt.datetime.now(dt.timezone.utc)).total_seconds())
            if remaining<=0 or task.get('stop_requested_at'):raise ValueError('Access expired')
            base=['kubectl','--context',self.config[0],'-n',self.config[1],'exec','-i','rq-'+task['id'],'-c','workspace','--']
            result=subprocess.run([*base,'python3','-I','-c',SSH_PREPARE],input=json.dumps(request),capture_output=True,text=True,timeout=12,check=True)
            if operation=='ssh-ready':return json.loads(result.stdout)
            return {'argv':[*base,'timeout','--signal=KILL',str(remaining),'/usr/sbin/sshd','-i','-f','/etc/requenta/sshd_config']}
        if operation=='command':
            pod=self.read_owned('pod',task)
            if not pod or pod.get('status',{}).get('phase')!='Running':raise ValueError('Workspace is not running')
            command=task['command']
            if not re.fullmatch(r'[a-f0-9-]{36}',command['id']) or command['kind'] not in ('probe','python','shell'):raise ValueError('Invalid command')
            end=dt.datetime.fromisoformat(task['end_at'].replace('Z','+00:00'))
            remaining=int((end-dt.datetime.now(dt.timezone.utc)).total_seconds())
            if remaining<=0:raise ValueError('Reservation expired')
            body={**command,'code':PROBE if command['kind']=='probe' else command['code'],'timeout':min(300,remaining),'runner':RUNNER}
            result=subprocess.run(['kubectl','--context',self.config[0],'-n',self.config[1],'exec','-i','rq-'+task['id'],'-c','workspace','--','python','-I','-c',DISPATCH],input=json.dumps(body),capture_output=True,text=True,timeout=12,check=True)
            return json.loads(result.stdout)
        return super().run(operation,task)


def main():
    try:print(json.dumps(Workspace(workspace_config()).run(sys.argv[1],json.load(sys.stdin)),ensure_ascii=False))
    except Exception as e:print(type(e).__name__,file=sys.stderr);raise SystemExit(1)


if __name__=='__main__':main()
