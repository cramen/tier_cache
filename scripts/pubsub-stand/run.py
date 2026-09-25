import json,time,threading,urllib.request,urllib.parse,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
report={'phases':[],'samples':[]}
stop=threading.Event()
def call(node,path,**query):
 base='http://127.0.0.1:'+str(18090+node)
 if query:path+='?'+urllib.parse.urlencode(query)
 with urllib.request.urlopen(base+path,timeout=90) as response:return response.read().decode()
def stats():return [json.loads(call(i,'/stats')) for i in range(1,7)]
def wait(check,timeout=60):
 end=time.monotonic()+timeout
 while True:
  try:
   if check():return
  except Exception:pass
  if time.monotonic()>end:raise AssertionError('condition timed out')
  time.sleep(.05)
def sample():
 while not stop.is_set():
  try:report['samples'].append({'at':time.time(),'nodes':stats()})
  except Exception:pass
  stop.wait(.05)
def phase(name,started):
 report['phases'].append({'name':name,'seconds':time.monotonic()-started,'nodes':stats()})
 (root/'results.json').write_text(json.dumps(report,indent=2))
 print(name,report['phases'][-1]['seconds'],flush=True)
def compose(*args):subprocess.run(['docker','compose','-f',str(root/'compose.yaml'),*args],check=True,capture_output=True)
try:
 wait(lambda:len(stats())==6)
 thread=threading.Thread(target=sample,daemon=True);thread.start()
 start=time.monotonic();call(1,'/put',cache='a',value='seed-a');call(1,'/put',cache='b',value='seed-b')
 wait(lambda:all(n['a']=='seed-a' and n['b']=='seed-b' for n in stats()))
 source_start=time.monotonic();call(0,'/get',key='origin-probe');report['sourceProbeSeconds']=time.monotonic()-source_start
 phase('warmup',start)
 start=time.monotonic();call(6,'/pause');call(1,'/put',cache='a',value='gated-first')
 wait(lambda:json.loads(call(6,'/stats'))['handlerPaused'])
 other=time.monotonic();call(1,'/put',cache='b',value='independent')
 wait(lambda:json.loads(call(6,'/stats'))['b']=='independent',5)
 report['independentCacheSeconds']=time.monotonic()-other
 assert json.loads(call(6,'/stats'))['handlerPaused']
 call(1,'/burst',cache='a',n=2000)
 wait(lambda:json.loads(call(6,'/stats'))['pendingA'])
 time.sleep(.3)
 paused=json.loads(call(6,'/stats'));assert paused['messages']<=16 and paused['bytes']<=65536 and paused['rejected']>0
 phase('paused-receiver-burst',start)
 start=time.monotonic();call(6,'/resume')
 wait(lambda:all(n['a']=='burst-1999' and not n['pendingA'] for n in stats()))
 phase('intact-history-repair',start)
 start=time.monotonic();call(6,'/pause');call(1,'/put',cache='a',value='before-clear')
 wait(lambda:json.loads(call(6,'/stats'))['handlerPaused'])
 call(1,'/burst',cache='a',n=300);call(1,'/clear',cache='a');call(1,'/put',cache='a',value='after-clear')
 call(6,'/resume');wait(lambda:all(n['a'] in (None,'after-clear') and not n['pendingA'] for n in stats()))
 report['afterEvictAllL1']=stats()
 for node in range(1,7):assert call(node,'/get',cache='a')=='after-clear'
 wait(lambda:all(n['a']=='after-clear' for n in stats()))
 phase('update-evict-all-repair',start)
 start=time.monotonic();compose('stop','redis');time.sleep(3)
 outage=[call(0,'/get',cache='b') for _ in range(12)];report['outageReadValues']=outage
 compose('start','redis');time.sleep(5)
 call(1,'/put',cache='a',value='restored');call(1,'/put',cache='b',value='restored-b')
 for node in range(1,7):
  wait(lambda node=node:call(node,'/get',cache='a')=='restored')
 call(1,'/put',cache='a',value='post-reconnect')
 wait(lambda:all(n['a']=='post-reconnect' and not n['pendingA'] and not n['pendingB'] for n in stats()))
 phase('redis-restart-and-new-mutation',start)
 assert all(n['messages']<=16 and n['bytes']<=65536 for sample in report['samples'] for n in sample['nodes'])
 report['passed']=True
finally:
 stop.set()
 try:call(6,'/resume')
 except Exception:pass
 (root/'results.json').write_text(json.dumps(report,indent=2))
