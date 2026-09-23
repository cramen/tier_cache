#!/usr/bin/env python3
"""Bounded Sentinel regressions. Only fixture-owned Docker resources are removed."""
import ipaddress, json, subprocess, time, uuid
from fixture_config import ROOT, PROFILES
IMAGE=PROFILES['redis74']
def command(args, timeout=30):
    p=subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if p.returncode: raise RuntimeError(f'{args}: {p.stderr} {p.stdout}')
    return p.stdout.strip()
def until(description, predicate, seconds=90):
    end=time.monotonic()+seconds
    last=None
    while time.monotonic()<end:
        try:
            result=predicate()
            if result:return result
        except (RuntimeError, subprocess.TimeoutExpired) as e:last=e
        time.sleep(.25)
    raise AssertionError(f'Timeout: {description}; last={last}')
def scenario(mode):
    command(['docker','pull',IMAGE],timeout=600)
    command(['docker','image','inspect','eclipse-temurin:17-jre'])
    token='tiercache-sentinel-'+uuid.uuid4().hex[:12]
    output=ROOT/'build/compatibility-evidence'/('sentinel-'+mode)
    output.mkdir(parents=True,exist_ok=True)
    control=output/token;control.mkdir()
    names=[];events=[]
    def event(kind,**kw):
        item=dict(time=time.time(),kind=kind,**kw);events.append(item)
        (output/'events.json').write_text(json.dumps(events,indent=2))
        print(item,flush=True)
    def start(alias,args,ip=None):
        name=token+'-'+alias;names.append(name)
        address=['--ip',ip] if ip else []
        command(['docker','run','-d','--name',name,'--network',token,'--network-alias',alias,*address,*args])
        return name
    def cli(name,*args):return command(['docker','exec',name,'redis-cli','--raw',*args],timeout=5)
    def topology():
        views=[cli(s,'-p','26379','SENTINEL','get-master-addr-by-name','mymaster').splitlines() for s in sentinels]
        if len({tuple(v) for v in views})!=1 or len(views[0])!=2:return None
        host,port=views[0]
        if port != "6379": return None
        for i,n in enumerate(nodes):
            if host==f'node{i}' or host in command(['docker','inspect','--format','{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',n]):
                if cli(n,'ROLE').splitlines()[0]=='master':return i
        return None
    try:
        network_ids=command(['docker','network','ls','-q']).splitlines()
        existing=json.loads(command(['docker','network','inspect',*network_ids])) if network_ids else []
        used=[ipaddress.ip_network(c['Subnet']) for n in existing for c in n.get('IPAM',{}).get('Config',[])
              if c.get('Subnet') and ':' not in c['Subnet']]
        candidates=[ipaddress.ip_network(f'10.254.{i}.0/24') for i in range(256)]
        network=next(n for n in candidates if not any(n.overlaps(u) for u in used))
        command(['docker','network','create','--subnet',str(network),token]);event('network',name=token,subnet=str(network))
        node_ips=[str(network.network_address+10+i) for i in range(3)]
        # Docker removes a stopped container's DNS name. Stable IPs let Sentinel
        # detect primary loss without blocking on DNS for the killed container.
        nodes=[]
        for i in range(3):
            args=[IMAGE,'redis-server','--appendonly','yes','--protected-mode','no','--replica-announce-ip',node_ips[i],
                  '--replica-announce-port','6379']
            if i:args+=['--replicaof',node_ips[0],'6379']
            nodes.append(start('node'+str(i),args,node_ips[i]))
        until('two replicas online',lambda:'connected_slaves:2' in cli(nodes[0],'INFO','replication'))
        sentinels=[]
        for i in range(3):
            confdir=control/('sentinel'+str(i));confdir.mkdir()
            conf=confdir/'sentinel.conf'
            conf.write_text('port 26379\nprotected-mode no\nsentinel resolve-hostnames yes\nsentinel announce-hostnames yes\n'
                +f'sentinel announce-ip sentinel{i}\nsentinel announce-port 26379\n'
                +f'sentinel monitor mymaster {node_ips[0]} 6379 2\nsentinel down-after-milliseconds mymaster 1500\n'
                +'sentinel failover-timeout mymaster 10000\nsentinel parallel-syncs mymaster 1\n')
            # Sentinel rewrites its config. Copy the read-only host template into
            # container-owned /data so Linux host/container UIDs need not match.
            sentinels.append(start('sentinel'+str(i),['-v',str(confdir)+':/config:ro',IMAGE,'sh','-c',
                'cp /config/sentinel.conf /data/sentinel.conf && '
                'exec /usr/local/bin/docker-entrypoint.sh redis-server /data/sentinel.conf --sentinel']))
        until('all sentinels agree',lambda:topology()==0)
        until('quorum discovered',lambda:all(cli(s,'-p','26379','SENTINEL','ckquorum','mymaster').startswith('OK') for s in sentinels))
        event('ready',master=0,image=IMAGE,imageId=command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']),
              jvmImageId=command(['docker','image','inspect','eclipse-temurin:17-jre','--format','{{.Id}}']))
        runtime=ROOT/'tiercache-spring-boot-starter/build/sentinel-runtime'
        probe=start('probe',['-v',str(runtime)+':/runtime:ro','-v',str(control)+':/control',
            'eclipse-temurin:17-jre','java','-cp','/runtime/classes:/runtime/lib/*','io.tiercache.spring.SentinelProbe',mode])
        def applications_ready():
            state=command(['docker','inspect','--format','{{.State.Status}}',probe])
            if state=='exited': raise AssertionError('Probe exited before readiness: '+command(['docker','logs',probe]))
            return (control/'ready').exists()
        until('applications ready',applications_ready)
        # WAIT on the probe writer connection would be stronger. Compare replication offsets
        # after its ready marker to prove both replicas contain all preceding writes.
        def replicated():
            info=dict(x.split(':',1) for x in cli(nodes[0],'INFO','replication').splitlines() if ':' in x)
            offset=int(info['master_repl_offset'])
            return all(int(dict(x.split(':',1) for x in cli(n,'INFO','replication').splitlines() if ':' in x)['slave_repl_offset'])>=offset for n in nodes[1:])
        until('retained history replicated',replicated)
        event('history-replicated')
        if mode=='planned':
            if cli(sentinels[0],'-p','26379','SENTINEL','failover','mymaster')!='OK':
                raise AssertionError('Sentinel did not accept planned failover')
        elif mode=='abrupt':command(['docker','kill','--signal','KILL',nodes[0]])
        else:
            for n in nodes:command(['docker','stop','-t','2',n])
            (control/'offline').write_text('ready')
            until('offline source update',lambda:(control/'offline-written').exists())
            for n in nodes:command(['docker','start',n])
        event('fault-injected',mode=mode)
        def elected_topology():
            primary=topology()
            if primary is not None and (mode=='outage' or primary!=0):
                return {'master':primary}
            return None
        elected=until('agreed elected primary',elected_topology)
        # Index zero is a valid master after an outage.
        elected=elected['master']
        event('elected',master=elected)
        (control/'topology-ready').write_text(str(elected))
        code=command(['docker','wait',probe],timeout=130)
        logs=subprocess.run(['docker','logs',probe],capture_output=True,text=True,timeout=10)
        (output/'probe.log').write_text(logs.stdout+logs.stderr)
        if code!='0' or not (control/'passed').exists():raise AssertionError('Probe failed, exit='+code)
        event('passed')
    finally:
        cleanup_errors=[]
        for name in names:
            try:
                p=subprocess.run(['docker','logs',name],capture_output=True,text=True,timeout=10)
                (output/(name+'.log')).write_text(p.stdout+p.stderr)
            except Exception as e: cleanup_errors.append(str(e))
        for name in reversed(names):
            try:
                p=subprocess.run(['docker','rm','-f','-v',name],capture_output=True,text=True,timeout=30)
                if p.returncode and 'No such container' not in p.stderr: cleanup_errors.append(p.stderr)
            except Exception as e: cleanup_errors.append(str(e))
        try:
            p=subprocess.run(['docker','network','rm',token],capture_output=True,text=True,timeout=30)
            if p.returncode and 'not found' not in p.stderr: cleanup_errors.append(p.stderr)
        except Exception as e: cleanup_errors.append(str(e))
        if cleanup_errors:
            (output/'cleanup-errors.json').write_text(json.dumps(cleanup_errors,indent=2))
            raise RuntimeError('Fixture cleanup incomplete: '+str(cleanup_errors))
if __name__=='__main__':
    import sys
    for mode in sys.argv[1:] or ['planned','abrupt','outage']:scenario(mode)
