#!/usr/bin/env python3
"""Summarize paired fork means with descriptive bootstrap intervals (three forks is a small sample)."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import random
import statistics

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('directories',type=Path,nargs='+')
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
groups=defaultdict(dict)
for directory in args.directories:
    for item in json.loads((directory/'index.json').read_text()):
        if item['profiled']: continue
        key=(item['kind'],item['jdk'],item['threads'],item['profile'],item['model'])
        identity=(item['fork'],item['version'])
        if identity in groups[key]: raise RuntimeError(f'Duplicate fork: {key} {identity}')
        raw=json.loads((directory/item['file']).read_text())
        metrics={'throughput':item['throughput']}
        if item['kind']=='jmh':
            metrics['cpuNsPerOp']=raw[0]['secondaryMetrics']['processCpu']['score']
            for name,data in raw[0]['secondaryMetrics'].items():
                if name.endswith('gc.alloc.rate.norm'):metrics['allocatedBytesPerOp']=data['score']
        else:
            rounds=raw['rounds']
            metrics['p99Ms']=statistics.median(r['p99Ms'] for r in rounds)
            metrics['cpuNsPerOp']=statistics.mean(r['processCpuNsPerOp'] for r in rounds)
            for field in ['workerAllocatedBytesPerOp','processAllocatedBytesPerOp']:
                values=[r[field] for r in rounds]
                metrics[field]=None if any(v is None for v in values) else statistics.mean(values)
            metrics['redisFraction']=sum(r['redisCalls'] for r in rounds)/sum(r['operations'] for r in rounds)
        groups[key][identity]=metrics
results=[]
for key,forks in sorted(groups.items()):
    baseline=[forks[(i,'baseline')] for i in [1,2,3]]
    candidate=[forks[(i,'candidate')] for i in [1,2,3]]
    comparison={}
    for metric in baseline[0]:
        a=[row[metric] for row in baseline];b=[row[metric] for row in candidate]
        if any(v is None for v in a+b):comparison[metric]={'baseline':a,'candidate':b,'available':False};continue
        entry={'baseline':a,'candidate':b,'baselineMedian':statistics.median(a),'candidateMedian':statistics.median(b)}
        if all(v>0 for v in a+b):
            ratios=[y/x for x,y in zip(a,b)]
            rng=random.Random(26092026)
            draws=sorted(math.exp(statistics.mean(math.log(rng.choice(ratios)) for _ in ratios)) for _ in range(10000))
            entry.update(pairedRatios=ratios,pairedGeometricMean=math.exp(statistics.mean(map(math.log,ratios))),
                         pairedBootstrap95=[draws[250],draws[9749]])
        comparison[metric]=entry
    results.append({'kind':key[0],'jdk':key[1],'threads':key[2],'profile':key[3],'model':key[4],'metrics':comparison})
args.output.parent.mkdir(parents=True,exist_ok=True)
args.output.write_text(json.dumps({'method':'paired fork means; medians and descriptive paired geometric-mean bootstrap interval; 10000 draws; fixed seed; n=3, not a universal significance claim','groups':results},indent=2)+'\n')
for r in results:
    m=r['metrics']['throughput'];ratio=m['candidateMedian']/m['baselineMedian']
    print(r['kind'],r['jdk'],r['threads'],r['profile'],r['model'],f'throughput ratio={ratio:.3f}',m.get('pairedBootstrap95'))
