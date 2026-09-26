#!/usr/bin/env python3
"""Run controlled and randomized Redis/Valkey convergence in fresh test JVMs."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runs', type=int, default=5)
parser.add_argument('--output', type=Path, default=Path('build/write-convergence'))
args = parser.parse_args()
if args.runs < 5:
    parser.error('acceptance requires at least five fresh runs')
repo = Path(__file__).resolve().parents[1]
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
command = ['./gradlew', '-I', 'scripts/convergence-runtime.gradle', ':tiercache-tck:test',
           '--tests', '*CommittedWriteConvergenceTest', '--tests', '*InvalidationRaceTest',
           '--offline', '--console=plain']
report = {'revision': subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
          'dirty': bool(subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True)),
          'command': command, 'runs': [], 'passed': False}
for index in range(1, args.runs + 1):
    run_dir = out / f'run-{index}'
    run_dir.mkdir(exist_ok=True)
    with (run_dir / 'gradle.log').open('w') as log:
        result = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=300)
    suites = []
    for path in (repo / 'tiercache-tck/build/test-results/test').glob('TEST-*.xml'):
        shutil.copy2(path, run_dir / path.name)
        suite = ET.parse(path).getroot()
        suites.append({name: suite.get(name) for name in ['name','tests','failures','errors','skipped','timestamp']})
    counts = {name: sum(int(s[name] or 0) for s in suites) for name in ['tests','failures','errors','skipped']}
    passed = result.returncode == 0 and counts == {'tests':6,'failures':0,'errors':0,'skipped':0}
    report['runs'].append({'run':index,'exitCode':result.returncode,'passed':passed,'suites':suites})
    (out / 'results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(index, counts, 'passed=', passed, flush=True)
    if not passed:
        raise SystemExit('Convergence failed; evidence retained, no retry-to-green.')
report['passed'] = True
(out / 'results.json').write_text(json.dumps(report,indent=2)+'\n')
