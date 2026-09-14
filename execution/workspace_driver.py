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
from kubernetes_driver import Kubernetes, configuration, manifests

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

def workspace_config():
    # Legacy gateway fields are unused for this outbound, command-based workspace driver.
    os.environ.setdefault('REQUENTA_ACCESS_ORIGIN','https://unused.invalid')
    os.environ.setdefault('REQUENTA_GATEWAY_NAMESPACE','requenta-unused')
    os.environ.setdefault('REQUENTA_GATEWAY_APP','unused')
    return configuration()


def workspace_manifests(task, config, now=None):
    policy,_,pod=manifests(task,config,now)
    policy['spec']['ingress']=[]
    container=pod['spec']['containers'][0]
    container.pop('ports');container.pop('readinessProbe')
    container['command']=['python','-I','-c','import time; time.sleep(2147483647)']
    container['env']=[{'name':'HOME','value':'/tmp'},{'name':'PYTHONDONTWRITEBYTECODE','value':'1'},{'name':'NVIDIA_DRIVER_CAPABILITIES','value':'compute,utility'}]
    container['readinessProbe']={'exec':{'command':['python','-I','-c',f'import torch; assert torch.cuda.is_available() and torch.cuda.device_count()=={int(task["gpus"])}']},'initialDelaySeconds':3,'periodSeconds':15,'timeoutSeconds':10,'failureThreshold':2}
    cap=os.environ.get('REQUENTA_WORKSPACE_MAX_LIFETIME_SECONDS')
    if cap:
        if not cap.isdigit() or not 60<=int(cap)<=86400:raise ValueError('Invalid local safety lifetime')
        pod['spec']['activeDeadlineSeconds']=min(pod['spec']['activeDeadlineSeconds'],int(cap))
    return [policy,pod]


class Workspace(Kubernetes):
    def run(self, operation, task):
        if operation=='reap':return self.reap()
        if task.get('access_mode')!='workspace':raise ValueError('Wrong access driver')
        if operation=='ensure':
            for resource in workspace_manifests(task,self.config):
                if not self.read_owned(resource['kind'].lower(),task):self.command(['create','-f','-','-o','json'],resource)
            return self.inspect(task)
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
