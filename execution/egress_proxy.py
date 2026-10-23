"""Allow-listed HTTPS CONNECT proxy for one booking (RFC-022). No TLS interception, no plain HTTP.
The workload reaches only this proxy; the proxy reaches only public addresses of allow-listed hosts on port 443.
Every forwarded byte is reserved first through the booking's directional transfer meter."""
import argparse
import asyncio
import ipaddress
import json
import os
import re
import socket
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

HEAD_LIMIT=8192
CHUNK=65536
IDLE_SECONDS=300
HOST=re.compile(r'(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+')


class Refused(Exception):
    def __init__(self,status,reason,host='-'):
        super().__init__(reason);self.status=status;self.reason=reason;self.host=host


def rule(value):
    """An exact hostname, or a leading dot for any subdomain (never the bare parent). No wildcards, no IPs."""
    name=value.strip().lower()
    host=name[1:] if name.startswith('.') else name
    if not HOST.fullmatch(host) or literal(host):raise ValueError('Invalid egress rule: '+value)
    return name


def literal(host):
    try:ipaddress.ip_address(host);return True
    except ValueError:return False


def allowed(host,rules):
    return any(host==r or (r.startswith('.') and host.endswith(r)) for r in rules)


def public(address):
    a=ipaddress.ip_address(address)
    if a.version==6 and a.ipv4_mapped:a=a.ipv4_mapped
    return a.is_global and not a.is_multicast


class Meter:
    """Bytes move only after a block is reserved for their direction. reserve(direction) returns the bytes granted,
    zero when the allowance is exhausted. ingress = into the workload (downloads), egress = out of it."""
    def __init__(self,reserve):
        self.reserve=reserve;self.available={'ingress':0,'egress':0};self.used={'ingress':0,'egress':0}
        self.locks={d:asyncio.Lock() for d in self.available}

    async def take(self,direction,size):
        async with self.locks[direction]:
            while self.available[direction]<size:
                granted=await self.reserve(direction)
                if granted<=0:return False
                self.available[direction]+=granted
            self.available[direction]-=size;self.used[direction]+=size;return True


class Proxy:
    def __init__(self,rules,meter,resolve=None,is_public=public,max_connections=64):
        self.rules=tuple(rule(r) for r in rules);self.meter=meter;self.is_public=is_public
        self.resolve=resolve or self.lookup;self.slots=asyncio.Semaphore(max_connections)

    async def lookup(self,host):
        infos=await asyncio.get_running_loop().getaddrinfo(host,443,type=socket.SOCK_STREAM)
        return list(dict.fromkeys(i[4][0] for i in infos))

    async def target(self,head):
        line=head.split(b'\r\n',1)[0].decode('ascii','replace').split(' ')
        if len(line)!=3 or not line[2].startswith('HTTP/1.'):raise Refused(400,'malformed')
        if line[0]!='CONNECT':raise Refused(405,'method')
        host,_,port=line[1].rpartition(':');host=host.lower().rstrip('.')
        if port!='443':raise Refused(403,'port')
        if literal(host.strip('[]')) or not HOST.fullmatch(host):raise Refused(403,'address')
        if not allowed(host,self.rules):raise Refused(403,'not_allowed',host)
        try:addresses=await asyncio.wait_for(self.resolve(host),10)
        except (OSError,asyncio.TimeoutError):raise Refused(502,'unresolved',host)
        # Every answer must be public: a mixed answer is how a rebinding attack reaches the cluster.
        if not addresses or not all(self.is_public(a) for a in addresses):raise Refused(403,'non_public',host)
        return host,addresses

    async def handle(self,reader,writer):
        host,result,started,upstream='-','refused',time.monotonic(),None
        used={'ingress':0,'egress':0}
        if self.slots.locked():
            writer.write(b'HTTP/1.1 503 Service Unavailable\r\n\r\n');await writer.drain();writer.close();return
        async with self.slots:
            try:
                try:head=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),10)
                except (asyncio.LimitOverrunError,asyncio.IncompleteReadError,asyncio.TimeoutError,ValueError):raise Refused(400,'malformed')
                host,addresses=await self.target(head)
                for address in addresses:
                    try:upstream=await asyncio.wait_for(asyncio.open_connection(address,443),10);break
                    except OSError:continue
                if not upstream:raise Refused(502,'unreachable',host)
                writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n');await writer.drain()
                result=await self.splice((reader,writer),upstream,used)
            except Refused as e:
                host,result=e.host,e.reason
                try:writer.write(f'HTTP/1.1 {e.status} Refused\r\n\r\n'.encode());await writer.drain()
                except OSError:pass
            except asyncio.TimeoutError:
                result='idle_timeout'
            except OSError:
                result='connection_error'
            finally:
                for w in (writer,upstream[1] if upstream else None):
                    if w:w.close()
                print(json.dumps({'host':host,'result':result,'ingress':used['ingress'],'egress':used['egress'],'seconds':round(time.monotonic()-started,3)}),flush=True)

    async def splice(self,client,upstream,used):
        async def pump(source,sink,direction):
            while chunk:=await asyncio.wait_for(source.read(CHUNK),IDLE_SECONDS):
                if not await self.meter.take(direction,len(chunk)):return 'allowance_exhausted'
                sink.write(chunk);await sink.drain();used[direction]+=len(chunk)
            return 'closed'
        jobs=[asyncio.create_task(pump(client[0],upstream[1],'egress')),asyncio.create_task(pump(upstream[0],client[1],'ingress'))]
        try:
            done,_=await asyncio.wait(jobs,return_when=asyncio.FIRST_COMPLETED)
            return next(iter(done)).result()
        finally:
            for job in jobs:job.cancel()
            await asyncio.gather(*jobs,return_exceptions=True)

    async def serve(self,host,port):
        return await asyncio.start_server(self.handle,host,port,limit=HEAD_LIMIT)


async def unmetered(direction):
    return 1<<20


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward the booking credential to another origin.


class HttpMeter:
    """Reserves blocks from the control plane with the per-booking egress credential. The token file is re-read on every
    call. A transient failure retries the same block id (idempotent server side); any refusal grants nothing."""
    def __init__(self,origin,token_file,attempts=3,timeout=10):
        u=urlparse(origin)
        if u.username or u.password or u.query or u.fragment or u.path not in ('','/') or not (u.scheme=='https' or (u.scheme=='http' and u.hostname in ('localhost','127.0.0.1'))):
            raise ValueError('Use an HTTPS control-plane origin or localhost')
        self.url=origin.rstrip('/')+'/api/execution/egress/blocks';self.token_file=Path(token_file)
        self.attempts=attempts;self.timeout=timeout;self.http=build_opener(NoRedirect())

    def call(self,direction,block):
        token=self.token_file.read_text().strip()
        if not re.fullmatch(r'rq_egress_[a-f0-9]{64}',token):raise ValueError('Invalid egress credential')
        req=Request(self.url,data=json.dumps({'blockId':block,'direction':direction}).encode(),method='POST',
                    headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with self.http.open(req,timeout=self.timeout) as response:
            granted=int(json.loads(response.read(4096))['bytes'])
        return granted if 0<granted<=1<<20 else 0

    async def __call__(self,direction):
        block=str(uuid4())
        for attempt in range(self.attempts):
            try:return await asyncio.to_thread(self.call,direction,block)
            except HTTPError as e:
                if e.code<500:return 0
            except (URLError,OSError,ValueError,KeyError):
                pass
            await asyncio.sleep(0.5*(attempt+1))
        return 0


def main():
    p=argparse.ArgumentParser();p.add_argument('--listen',default='0.0.0.0');p.add_argument('--port',type=int,default=3128);a=p.parse_args()
    rules=[r for r in os.environ.get('REQUENTA_EGRESS_ALLOW','').split(',') if r.strip()]
    if not rules:raise SystemExit('REQUENTA_EGRESS_ALLOW is empty: nothing to proxy')
    # The driver supplies the booking meter (RFC-022). Unmetered runs are an explicit local-development choice.
    mode=os.environ.get('REQUENTA_EGRESS_METER')
    if mode=='http':reserve=HttpMeter(os.environ.get('REQUENTA_ORIGIN',''),os.environ.get('REQUENTA_EGRESS_TOKEN_FILE','/var/run/requenta-egress/token'))
    elif mode=='none-development':reserve=unmetered
    else:raise SystemExit('No transfer meter configured; refusing to start')
    async def run():
        server=await Proxy(rules,Meter(reserve)).serve(a.listen,a.port)
        async with server:await server.serve_forever()
    asyncio.run(run())


if __name__=='__main__':
    main()
