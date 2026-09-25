#!/usr/bin/env python3
"""Compare core jars with identical async workload settings and separate JVM forks."""
import argparse
import json
import pathlib
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=pathlib.Path, required=True)
parser.add_argument('--candidate', type=pathlib.Path, required=True)
parser.add_argument('--slf4j', type=pathlib.Path, required=True)
parser.add_argument('--java', default='java')
parser.add_argument('--output', type=pathlib.Path, required=True)
parser.add_argument('--forks', type=int, default=3)
args = parser.parse_args()
repo = pathlib.Path(__file__).resolve().parents[1]
args.output.mkdir(parents=True, exist_ok=True)
fixtures = repo / 'tiercache-core/build/classes/java/testFixtures'
classes = repo / 'tiercache-core/build/classes/java/test'
results = []
for fork in range(args.forks):
    for profile in ['l1', 'hot', 'miss', 'l2', 'failure']:
        versions = ['baseline', 'candidate'] if fork % 2 == 0 else ['candidate', 'baseline']
        for version in versions:
            jar = getattr(args, version).resolve()
            classpath = ':'.join(map(str, [classes, jar, fixtures, args.slf4j.resolve()]))
            command = [args.java, '-Xms512m', '-Xmx512m', '-cp', classpath,
                       'io.tiercache.AsyncFollowerBenchmark', profile]
            run = subprocess.run(command, capture_output=True, text=True)
            name = f'{version}-{profile}-{fork}'
            (args.output / f'{name}.log').write_text(run.stdout + run.stderr)
            if run.returncode:
                raise RuntimeError(f'{name} exited {run.returncode}; see its log')
            row = json.loads(run.stdout.strip().splitlines()[-1])
            row.update(version=version, fork=fork, command=command)
            results.append(row)
            (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
            print(name, round(row['opsPerSecond'], 1), row['unrelatedP99Ms'], flush=True)
