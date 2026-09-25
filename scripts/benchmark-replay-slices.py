#!/usr/bin/env python3
"""Compare frozen baseline/candidate runtimes against a dedicated Redis instance.

Export runtime classpaths with scripts/pubsub-runtime.gradle. Freeze baseline
jars/classes before building the candidate; a classpath into a mutable build
folder is not a baseline. Both runtimes must include ReplaySliceBenchmark.
"""
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-classpath', type=Path, required=True)
parser.add_argument('--candidate-classpath', type=Path, required=True)
parser.add_argument('--java', default='java')
parser.add_argument('--redis', default='redis://127.0.0.1:16390')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--forks', type=int, default=3)
parser.add_argument('--jfr', action='store_true', help='Separate profiling run; do not score these latencies')
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
if args.jfr:
    settings = args.output.resolve() / 'monitors.jfc'
    settings.write_text('''<?xml version="1.0" encoding="UTF-8"?>
<configuration version="2.0" label="Replay monitors" description="Separate attribution run" provider="TierCache">
 <event name="jdk.JavaMonitorEnter">
  <setting name="enabled">true</setting>
  <setting name="stackTrace">true</setting>
  <setting name="threshold">0 ns</setting>
 </event>
</configuration>
''')
results = []
for profile in (['ordinary'] if args.jfr else ['ordinary', 'healthy', 'clear']):
    for fork in range(1, args.forks + 1):
        for version in (['baseline', 'candidate'] if fork % 2 else ['candidate', 'baseline']):
            label = f'{profile}-{fork}-{version}'
            cp_file = getattr(args, version + '_classpath')
            command = [args.java, '-Xms512m', '-Xmx512m']
            if args.jfr:
                recording = args.output.resolve() / (label + '.jfr')
                command += [f'-XX:StartFlightRecording=filename={recording},settings={settings},dumponexit=true']
            command += ['-cp', cp_file.read_text().strip(), 'io.tiercache.redis.ReplaySliceBenchmark', args.redis, profile]
            run = subprocess.run(command, text=True, capture_output=True, timeout=180)
            (args.output / (label + '.log')).write_text(run.stdout + run.stderr)
            run.check_returncode()
            result = json.loads(run.stdout.strip().splitlines()[-1])
            result.update(version=version, fork=fork, profiled=args.jfr, command=command)
            (args.output / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
            results.append(result)
            print(f'{label}: p99={result["sameP99Ms"]:.3f} ms; coherent={result["coherent"]}', flush=True)
(args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
