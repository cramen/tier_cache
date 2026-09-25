#!/usr/bin/env python3
"""Run equal-profile real Redis Pub/Sub comparisons in separate JVM forks."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-classpath', type=Path, required=True)
parser.add_argument('--candidate-classpath', type=Path, required=True)
parser.add_argument('--benchmark-classes', type=Path, default=Path('tiercache-transport-redis/build/classes/java/test'))
parser.add_argument('--redis-uri', default='redis://127.0.0.1:16389')
parser.add_argument('--java', default='java')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--forks', type=int, default=3)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
plan = {'forks': args.forks, 'healthyTargetRate': 1000, 'healthyMessages': 5000,
        'burstMessages': 2001, 'heapMiB': 512, 'healthyThroughputRegressionLimitPercent': 10}
(args.output / 'plan.json').write_text(json.dumps(plan, indent=2))
rows = []
for fork in range(args.forks):
    for mode in ['healthy', 'burst']:
        versions = ['baseline', 'candidate'] if fork % 2 == 0 else ['candidate', 'baseline']
        for version in versions:
            path = getattr(args, version + '_classpath')
            classpath = str(args.benchmark_classes.resolve()) + os.pathsep + path.read_text().strip()
            command = [args.java, '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
                       '-Xms512m', '-Xmx512m', '-cp', classpath,
                       'io.tiercache.redis.PubSubDispatchBenchmark', args.redis_uri, mode]
            result = subprocess.run(command, capture_output=True, text=True, timeout=90)
            name = f'{version}-{mode}-{fork}'
            (args.output / (name + '.log')).write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f'{name} failed; see its log')
            row = json.loads(result.stdout.strip().splitlines()[-1])
            recorded_command = ["<redis-uri>" if item == args.redis_uri else item for item in command]
            row.update(version=version, fork=fork, command=recorded_command)
            rows.append(row)
            (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
            print(name, row['throughput'], row['p99Ms'], row['peakMessages'], flush=True)
healthy = {version: statistics.mean(row['throughput'] for row in rows
           if row['version'] == version and row['mode'] == 'healthy')
           for version in ['baseline', 'candidate']}
assert healthy['candidate'] >= healthy['baseline'] * .9, healthy
for row in rows:
    if row['version'] == 'candidate':
        assert row['peakMessages'] <= 1024 and row['peakEncodedBytes'] <= 16777216
        assert row['peakControlGroups'] <= 1024
        if row['mode'] == 'burst':
            assert row['independentBeforeRelease']
