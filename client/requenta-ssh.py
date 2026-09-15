#!/usr/bin/env python3
"""Requenta SSH transport. Private keys never leave OpenSSH on your computer."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import urlparse
from uuid import uuid4

UUID=r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'

def read_config(path):
    if path.stat().st_size>4096:raise ValueError('Invalid connection file')
    config=json.loads(path.read_text())
    u=urlparse(config['origin'])
    if (u.scheme!='https' and not (u.scheme=='http' and u.hostname in ('localhost','127.0.0.1'))) or not u.hostname or u.username or u.password or u.path not in ('','/') or u.query or u.fragment:raise ValueError('Expected HTTPS console origin')
    for field in ('id','bookingId'):
        if not re.fullmatch(UUID,config[field]):raise ValueError('Invalid connection identifier')
    if not re.fullmatch(r'rq_ssh_[a-f0-9]{64}',config['token']):raise ValueError('Invalid access credential')
    if not re.fullmatch(r'ssh-ed25519 [A-Za-z0-9+/]{68}',config['hostKey']):raise ValueError('Invalid host key')
    return config

def quote(path):
    value=str(path)
    if any(c in value for c in '\r\n%'):raise ValueError('Unsupported path character')
    return '"'+value.replace('\\','\\\\').replace('"','\\"')+'"'

def setup(source,identity):
    config=read_config(source)
    if os.name!='posix':raise ValueError('This helper currently supports macOS and Linux')
    identity=identity.expanduser().resolve(strict=True)
    root=Path.home()/'.requenta'/'ssh';root.mkdir(parents=True,exist_ok=True,mode=0o700);root.chmod(0o700)
    venv=root/'venv'
    if not (venv/'bin/python').exists():subprocess.run([sys.executable,'-m','venv',str(venv)],check=True)
    python=venv/'bin/python'
    subprocess.run([str(python),'-m','pip','install','--disable-pip-version-check','websockets==17.1','certifi==2026.7.22'],check=True)
    helper=root/'proxy.py';shutil.copyfile(Path(__file__),helper);helper.chmod(0o600)
    directory=root/config['bookingId'];directory.mkdir(exist_ok=True,mode=0o700)
    session=directory/'session.json';session.write_text(json.dumps(config));session.chmod(0o600)
    alias='requenta-'+config['bookingId']
    known=directory/'known_hosts';known.write_text(alias+' '+config['hostKey']+'\n');known.chmod(0o600)
    host=directory/'config'
    # ProxyCommand is evaluated by a shell; quote every local path with shlex in addition to SSH quoting.
    import shlex
    proxy=' '.join(shlex.quote(str(p)) for p in (python,helper,'proxy',session))
    if '%' in proxy or '\n' in proxy or '\r' in proxy:raise ValueError('Unsupported local path')
    host.write_text(f'Host {alias}\n  HostName {alias}\n  User requenta\n  IdentityFile {quote(identity)}\n  IdentitiesOnly yes\n  HostKeyAlias {alias}\n  UserKnownHostsFile {quote(known)}\n  StrictHostKeyChecking yes\n  PasswordAuthentication no\n  KbdInteractiveAuthentication no\n  ProxyCommand {proxy}\n  ServerAliveInterval 15\n  ServerAliveCountMax 2\n')
    host.chmod(0o600)
    ssh=Path.home()/'.ssh';ssh.mkdir(exist_ok=True,mode=0o700)
    sshconfig=ssh/'config';include='Include '+quote(root/'*'/'config')
    current=sshconfig.read_text() if sshconfig.exists() else ''
    if include not in current.splitlines():
        if sshconfig.exists():shutil.copy2(sshconfig,root/'ssh-config.backup')
        sshconfig.write_text(include+'\n'+current);sshconfig.chmod(0o600)
    print('\nConnect: ssh '+alias)
    print('VS Code: Remote-SSH: Connect to Host → '+alias)
    print('Open /workspace. Access expires at '+config['expiresAt']+'.')
    print('Delete the downloaded connection JSON after setup; it contains a temporary credential.')

async def proxy(path):
    from websockets.asyncio.client import connect
    config=read_config(path)
    url=config['origin'].rstrip('/').replace('https://','wss://',1).replace('http://','ws://',1)+'/access/ssh/client/'+config['id']+'/'+str(uuid4())
    import ssl,certifi
    tls={'ssl':ssl.create_default_context(cafile=certifi.where())} if url.startswith('wss://') else {}
    async with connect(url,**tls,additional_headers={'Authorization':'Bearer '+config['token']},compression=None,max_size=65536,max_queue=4,proxy=None,close_timeout=1) as ws:
        loop=asyncio.get_running_loop();reader=asyncio.StreamReader();protocol=asyncio.StreamReaderProtocol(reader)
        transport,_=await loop.connect_read_pipe(lambda:protocol,sys.stdin.buffer)
        async def upload():
            while chunk:=await reader.read(32768):await ws.send(chunk)
        async def download():
            async for chunk in ws:
                if not isinstance(chunk,bytes):raise ValueError('Invalid tunnel data')
                # A pipe may apply backpressure; never block the event loop.
                await asyncio.to_thread(write_stdout,chunk)
        jobs=[asyncio.create_task(upload()),asyncio.create_task(download())]
        try:await asyncio.wait(jobs,return_when=asyncio.FIRST_COMPLETED)
        finally:
            transport.close()
            for job in jobs:job.cancel()
            await asyncio.gather(*jobs,return_exceptions=True)

def write_stdout(chunk):
    sys.stdout.buffer.write(chunk);sys.stdout.buffer.flush()

def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    s=sub.add_parser('setup');s.add_argument('config',type=Path);s.add_argument('--identity',required=True,type=Path)
    s=sub.add_parser('proxy');s.add_argument('config',type=Path)
    a=p.parse_args()
    try:
        if a.action=='setup':setup(a.config,a.identity)
        else:asyncio.run(proxy(a.config))
    except Exception:
        print('Requenta SSH could not connect. Check the session, connection expiry and local setup.',file=sys.stderr)
        raise SystemExit(1)

if __name__=='__main__':main()
