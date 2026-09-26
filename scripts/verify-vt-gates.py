#!/usr/bin/env python3
"""Run the complete VT gate three times on each explicitly selected runtime."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, default=Path('build/vt-validation'))
args = parser.parse_args()
repo = Path(__file__).resolve().parents[1]
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
results = []
pids = set()
for jdk in (21, 25):
    for fork in range(1, 4):
        run = output / f'jdk-{jdk}-fork-{fork}'
        run.mkdir()  # Never merge a new run with old evidence.
        command = ['./gradlew', ':tiercache-tck:vtStressTest', f'-PtiercacheVtJdk={jdk}',
                   f'-PtiercacheVtOutput={run}', '--offline', '--console=plain', '--continue']
        item = {'jdk': jdk, 'fork': fork, 'passed': False, 'phases': []}
        try:
            with (run / 'gradle.log').open('w') as log:
                process = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=420)
            item['exitCode'] = process.returncode
            for phase, status in [('cold', 'DIAGNOSTIC_COMPLETE'), ('steady', 'PASS')]:
                paths = list((run / f'jdk-{jdk}' / phase).glob('*/summary.json'))
                assert len(paths) == 1, (phase, paths)
                data = json.loads(paths[0].read_text())
                assert data['phase'] == phase and data['status'] == status, data
                assert int(data['requestedJdk']) == jdk and data['javaVersion'].split('.')[0].split('+')[0] == str(jdk)
                assert data['pid'] not in pids, 'phase JVM was reused'
                pids.add(data['pid'])
                measured = data['measured']
                assert measured['submitted'] == measured['completed'] == 100000
                assert measured['operations'] == 2000000 and measured['loaderExecutions'] > 0
                assert measured['initialLoaderKeys'] == 0 and measured['verifiedAbsentLoaderKeys'] == 1000
                assert measured['terminated'] and paths[0].with_name('recording.jfr').stat().st_size > 0
                if phase == 'steady':
                    assert data['events']['product'] == 0
                    assert data['warmup']['completed'] == 1000 and data['warmup']['operations'] == 20000
                item['phases'].append({key: data[key] for key in ['runId','phase','status','pid','javaVersion','javaVendor','measured','events']})
            suites = []
            for task in ['vtColdStartTest', 'vtSteadyStateTest', 'vtStressTest']:
                target = run / task
                target.mkdir()
                for path in (repo / 'tiercache-tck/build/test-results' / task).glob('TEST-*.xml'):
                    shutil.copy2(path, target / path.name)
                    suite = ET.parse(path).getroot()
                    suites.append({key: suite.get(key) for key in ['name','tests','failures','errors','skipped']})
            assert sum(int(s['tests']) for s in suites) >= 6
            assert all(int(s[k]) == 0 for s in suites for k in ['failures','errors','skipped'])
            assert process.returncode == 0
            item['suites'] = suites
            item['passed'] = True
        except Exception as error:
            item['failure'] = repr(error)
        results.append(item)
        (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(jdk, fork, 'passed=', item['passed'], flush=True)
        if not item['passed']:
            raise SystemExit('VT acceptance failed; evidence retained, no retry-to-green.')
