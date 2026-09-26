#!/usr/bin/env python3
"""Paired immutable-runtime comparisons; all timed processes run sequentially.

Export JMH and Redis classpaths, then copy every entry before invoking this
runner. A classpath that still points at mutable build outputs is not a baseline.
Profiler runs are separate and must not be scored as latency improvements.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
for version in ['baseline', 'candidate']:
    for kind in ['jmh', 'redis']:
        parser.add_argument(f'--{version}-{kind}-classpath', type=Path, required=True)
for jdk in [17, 21, 25]:
    parser.add_argument(f'--java{jdk}', required=True)
parser.add_argument('--redis', default='redis://127.0.0.1:16391')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--resume', action='store_true', help='Reuse only completed indexed measurements; refuses unaccounted files')
parser.add_argument('--section', choices=['target','micro','micro-rest','redis','representative','jfr'], required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
index_path = args.output / 'index.json'
results = json.loads(index_path.read_text()) if args.resume and index_path.exists() else []
completed = {item['file'] for item in results}
settings = args.output.resolve() / 'attribution.jfc'
if args.section == 'jfr':
    settings.write_text('''<?xml version="1.0" encoding="UTF-8"?>
<configuration version="2.0" label="Breaker attribution" description="Separate from scored runs" provider="TierCache">
 <event name="jdk.JavaMonitorEnter"><setting name="enabled">true</setting><setting name="stackTrace">true</setting><setting name="threshold">0 ns</setting></event>
 <event name="jdk.ExecutionSample"><setting name="enabled">true</setting><setting name="period">5 ms</setting></event>
</configuration>
''')

def compare(kind, jdk, threads, profile, model='platform', diagnostic=False):
    for fork in range(1, 2 if args.section == 'jfr' else 4):
        versions = ['baseline','candidate'] if fork % 2 else ['candidate','baseline']
        for version in versions:
            label = f'{kind}-{jdk}-{threads}-{profile}-{model}-{fork}-{version}'
            path = args.output / (label + '.json')
            if path.exists() and path.name not in completed:
                raise RuntimeError('Refusing to overwrite unaccounted evidence: ' + str(path))
            cp_path = getattr(args, f'{version}_{kind}_classpath')
            cp = cp_path.read_text().strip()
            java = getattr(args, f'java{jdk}')
            command = [java, '-Xms512m', '-Xmx512m']
            recording = args.output.resolve() / (label + '.jfr')
            jfr = f'-XX:StartFlightRecording=filename={recording},settings={settings},dumponexit=true'
            if kind == 'redis' and args.section == 'jfr':
                command += [jfr]
            command += ['-cp', cp]
            if kind == 'jmh':
                benchmark = ('io.tiercache.jmh.ClosedAdmissionDiagnostic.admit' if diagnostic
                             else 'io.tiercache.jmh.ClosedBreakerBenchmark.guardedAttempt')
                command += ['org.openjdk.jmh.Main', benchmark, '-t',str(threads),'-f','1',
                            '-wi','3','-w','2s','-i','3','-r','2s','-rf','json','-rff',str(path.resolve()),
                            '-jvm',java,'-jvmArgs','-Xms512m -Xmx512m','-prof','gc','-prof','io.tiercache.jmh.ProcessCpuProfiler']
                if not diagnostic:
                    command += ['-p',f'workload={profile}']
                if args.section == 'jfr':
                    command += ['-jvmArgsAppend',jfr]
            else:
                command += ['io.tiercache.redis.ClosedBreakerRedisBenchmark',args.redis,profile,str(threads),model]
            if path.name in completed:
                metadata=json.loads((args.output / (label + '.command.json')).read_text())
                if metadata['exitCode']!=0 or not path.exists() or metadata['command']!=command:
                    raise RuntimeError('Resume parameters/evidence changed; use a new output directory: '+label)
                print('Retained completed measurement',label,flush=True)
                continue
            meta = {'kind':kind,'jdk':jdk,'threads':threads,'profile':profile,'model':model,'fork':fork,
                    'version':version,'profiled':args.section=='jfr','command':command,
                    'classpathSha256':hashlib.sha256(cp.encode()).hexdigest()}
            log_path = args.output / (label + '.log')
            with log_path.open('w') as log:
                process = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240)
            meta['exitCode'] = process.returncode
            (args.output / (label + '.command.json')).write_text(json.dumps(meta,indent=2)+'\n')
            if process.returncode:
                raise RuntimeError(f'{label} failed; see {log_path}')
            if kind == 'redis':
                data = json.loads(next(line for line in reversed(log_path.read_text().splitlines()) if line.startswith('{')))
                assert data['coherent']
                path.write_text(json.dumps(data,indent=2)+'\n')
                score = sum(r['throughput'] for r in data['rounds']) / len(data['rounds'])
            else:
                data = json.loads(path.read_text())
                assert len(data)==1 and data[0]['primaryMetric']['score']>0, label
                score = data[0]['primaryMetric']['score']
            results.append({**{k:v for k,v in meta.items() if k!='command'},'file':path.name,'throughput':score})
            (args.output / 'index.json').write_text(json.dumps(results,indent=2)+'\n')
            print(label,round(score,3),flush=True)

if args.section in ['target','micro']:
    for threads in [32,1]:
        compare('jmh',21,threads,'healthy')
if args.section in ['micro','micro-rest']:
    for threads in [8,64]:
        compare('jmh',21,threads,'healthy')
    for profile in ['mixed','open','saturated']:
        for threads in [1,8,32,64]:
            compare('jmh',21,threads,profile)
    for threads in [1,32]:
        compare('jmh',21,threads,'admission-only',diagnostic=True)
if args.section == 'redis':
    for profile in ['l2','multi','mixed','hot']:
        compare('redis',21,16,profile)
    compare('redis',21,1,'l2')
if args.section == 'representative':
    for jdk in [17,25]:
        for threads in [1,32]:
            compare('jmh',jdk,threads,'healthy')
        compare('redis',jdk,16,'l2')
    for jdk in [21,25]:
        compare('redis',jdk,16,'l2','virtual')
if args.section == 'jfr':
    compare('jmh',21,32,'healthy')
    compare('redis',21,16,'multi')
