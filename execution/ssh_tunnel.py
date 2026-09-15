"""Outbound SSH channels. Only the selected booking pod is reachable."""
import asyncio
import datetime as dt
import json
import re
import threading
import time
from websockets.asyncio.client import connect

UUID=re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')

class SshManager:
    def __init__(self,worker):
        self.worker=worker
        self.running={}

    def update(self,task,grants):
        wanted={g['id']:g for g in grants}
        for ident,(booking,stop,thread) in list(self.running.items()):
            if not thread.is_alive() or (booking==task['id'] and ident not in wanted):
                stop.set();del self.running[ident]
        for ident,grant in wanted.items():
            if ident not in self.running:
                stop=threading.Event()
                thread=threading.Thread(target=lambda t=task,g=grant,s=stop:asyncio.run(self.run(t,g,s)),daemon=True)
                self.running[ident]=(task['id'],stop,thread)
                thread.start()

    async def run(self,task,grant,stop):
        ident=grant['id']
        if not UUID.fullmatch(ident):return
        expiry=dt.datetime.fromisoformat(grant['expires_at'].replace('Z','+00:00')).timestamp()
        url=self.worker.origin.replace('https://','wss://',1).replace('http://','ws://',1)+'/access/ssh/provider/'+ident
        while not stop.is_set() and time.time()<expiry:
            channels=set()
            try:
                token=self.worker.token_file.read_text().strip()
                async with connect(url,additional_headers={'Authorization':'Bearer '+token},compression=None,max_size=65536,max_queue=4,proxy=None,close_timeout=1) as ws:
                    while not stop.is_set() and time.time()<expiry:
                        try: data=await asyncio.wait_for(ws.recv(),1)
                        except asyncio.TimeoutError:continue
                        message=json.loads(data)
                        channel=message.get('channel','')
                        if not UUID.fullmatch(channel) or len(channels)>=4:raise ValueError('Invalid channel')
                        job=asyncio.create_task(self.bridge(url+'/'+channel,token,{**task,'ssh_grant':grant}))
                        channels.add(job);job.add_done_callback(channels.discard)
            except Exception:
                pass
            finally:
                for job in list(channels):job.cancel()
                await asyncio.gather(*channels,return_exceptions=True)
            await asyncio.sleep(1)

    async def bridge(self,url,token,task):
        process=None
        try:
            spec=await asyncio.to_thread(self.worker.operate,'ssh-spec',task)
            async with connect(url,additional_headers={'Authorization':'Bearer '+token},compression=None,max_size=65536,max_queue=4,proxy=None,close_timeout=1) as ws:
                process=await asyncio.create_subprocess_exec(*spec['argv'],stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
                async def upload():
                    while chunk:=await process.stdout.read(32768):await ws.send(chunk)
                async def download():
                    async for chunk in ws:
                        if not isinstance(chunk,bytes):raise ValueError('Binary channel required')
                        process.stdin.write(chunk);await process.stdin.drain()
                jobs=[asyncio.create_task(upload()),asyncio.create_task(download())]
                try:await asyncio.wait(jobs,return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for job in jobs:job.cancel()
                    await asyncio.gather(*jobs,return_exceptions=True)
        except Exception:
            pass
        finally:
            if process:
                if process.returncode is None:
                    try:process.kill()
                    except ProcessLookupError:pass
                await process.wait()
