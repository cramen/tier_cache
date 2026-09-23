#!/usr/bin/env python3
"""Fail-closed dependency evidence and exact-byte release manifests (stdlib only)."""
import argparse
import datetime as dt
import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

MODULES = {'tiercache-'+name for name in ('core','invalidation','transport-redis','spring-boot-starter',
                                         'micrometer','kotlin','reactor','micronaut','tck')}
GROUP = 'io.github.cramen'
class EvidenceError(RuntimeError): pass

def require(condition, message):
    if not condition: raise EvidenceError(message)
def read(path):
    with Path(path).open() as source: return json.load(source)
def write(path, value):
    path=Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')
def sha(path):
    with Path(path).open('rb') as source: return hashlib.file_digest(source, 'sha256').hexdigest()
def coordinate(item): return (item['group'], item['name'], item['version'])
def purl(coord): return 'pkg:maven/'+urllib.parse.quote(coord[0],safe='.')+'/'+urllib.parse.quote(coord[1],safe='')+'@'+urllib.parse.quote(coord[2],safe='.-')
def from_purl(value):
    require(isinstance(value,str) and value.startswith('pkg:maven/'), 'Missing Maven package identity')
    body=value.removeprefix('pkg:maven/').split('?',1)[0].split('#',1)[0]
    name, version=body.rsplit('@',1); group,name=name.rsplit('/',1)
    return tuple(urllib.parse.unquote(x) for x in (group,name,version))
def local(root, name):
    path=(root/name).resolve()
    require(path.is_relative_to(root.resolve()) and path.is_file(), 'Missing or unsafe input: '+str(name))
    return path

def validate_sbom(document, expected, identity):
    require(document.get('bomFormat')=='CycloneDX', 'Not a CycloneDX document')
    require(coordinate(document['metadata']['component'])==identity, 'SBOM root identity mismatch')
    components=document.get('components')
    require(isinstance(components,list) and components, 'Empty relevant SBOM components')
    observed={coordinate(c) for c in components if all(k in c for k in ('group','name','version'))}
    observed.add(identity)
    missing=set(expected)-observed
    require(not missing, 'SBOM omits resolved components: '+str(sorted(missing)))
    for c in components:
        if c.get('group') and c.get('name') and c.get('version'):
            require(from_purl(c.get('purl'))==coordinate(c), 'SBOM PURL/coordinate mismatch')
    return observed

def validate_inventory(root, inventory):
    require(inventory.get('schemaVersion')==1 and inventory.get('group')==GROUP, 'Invalid inventory identity')
    version=inventory['version']
    require(isinstance(version,str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.\-]*',version), 'Unsafe version')
    modules=inventory.get('modules',[])
    require(len(modules)==len(MODULES) and {m['name'] for m in modules}==MODULES, 'Incomplete module inventory')
    all_components=set(); sboms={}; expected_packages={}
    for module in modules:
        name=module['name']; configs=module['configurations']
        expected_configs={'runtimeClasspath'}
        if name=='tiercache-core': expected_configs.add('testFixturesRuntimeClasspath')
        if name=='tiercache-tck': expected_configs.add('testRuntimeClasspath')
        require(set(configs)==expected_configs, 'Missing classifier runtime graph: '+name)
        deps={coordinate(a) for artifacts in configs.values() for a in artifacts}
        require(deps, 'Empty resolved inventory: '+name)
        all_components.update(deps); all_components.add((GROUP,name,version))
        publications=module['publications']; require(len(publications)==1, 'Unexpected publication count')
        publication=publications[0]
        require(coordinate(publication)==(GROUP,name,version), 'Publication coordinates do not match project')
        expected_classifiers={'','sources','javadoc'}
        if name=='tiercache-core': expected_classifiers|={'unshaded','test-fixtures','test-fixtures-sources'}
        if name=='tiercache-tck': expected_classifiers.add('tests')
        files=publication['files']
        require(len(files)==len(expected_classifiers)+2, 'Unexpected publication artifact count: '+name)
        require({f['classifier'] for f in files if f['extension']=='jar'}==expected_classifiers, 'Missing/extra classifier: '+name)
        require({f['extension'] for f in files}=={'jar','pom','module'}, 'Missing publication metadata')
        by_name={}
        for file in files:
            path=local(root,file['path']); require(sha(path)==file['sha256'], 'Input checksum changed: '+str(path))
            if file['extension']=='jar': by_name[path.name]=file
            elif file['extension']=='pom':
                pom=ET.parse(path).getroot(); ns='{http://maven.apache.org/POM/4.0.0}'
                require(tuple(pom.findtext(ns+k) for k in ('groupId','artifactId','version'))==(GROUP,name,version), 'POM identity mismatch')
        metadata=read(local(root,next(f['path'] for f in files if f['extension']=='module')))
        comp=metadata['component']
        require((comp['group'],comp['module'],comp['version'])==(GROUP,name,version), 'Gradle metadata identity mismatch')
        for variant in metadata['variants']:
            for file in variant.get('files',[]):
                require(file['name'] in by_name, 'Metadata references unstaged artifact: '+file['name'])
                require(file.get('sha256')==by_name[file['name']]['sha256'], 'Metadata checksum mismatch')
        if name=='tiercache-core':
            caffeine={c for c in deps if c[:2]==('com.github.ben-manes.caffeine','caffeine')}
            require(len(caffeine)==1, 'Missing/ambiguous embedded Caffeine inventory')
            require(any(c[:2]==('org.slf4j','slf4j-api') for c in deps), 'Missing SLF4J inventory')
            main=next(f for f in files if f['extension']=='jar' and not f['classifier'])
            with zipfile.ZipFile(local(root,main['path'])) as jar:
                require('io/tiercache/internal/caffeine/cache/Cache.class' in jar.namelist(), 'Shaded Caffeine missing from main jar')
        if name=='tiercache-transport-redis':
            require(any(c[:2]==('io.lettuce','lettuce-core') for c in deps), 'Missing transport client inventory')
        doc=read(local(root,module['sbom']))
        observed=validate_sbom(doc,deps,(GROUP,name,version))
        sboms[name]=module['sbom']; expected_packages[name]={c for c in observed if c[0]!=GROUP}
    aggregate=read(local(root,inventory['aggregateSbom']))
    observed=validate_sbom(aggregate,all_components,(GROUP,inventory['name'],version))
    sboms['aggregate']=inventory['aggregateSbom']
    expected_packages['aggregate']={c for c in observed if c[0]!=GROUP}
    return sboms,expected_packages

def validate_scan(document, expected):
    require(document.get('SchemaVersion')==2 and document.get('ArtifactType')=='cyclonedx', 'Invalid/unparsed Trivy SBOM report')
    results=document.get('Results'); require(isinstance(results,list) and results, 'Empty Trivy results')
    packages=set(); findings=[]
    for result in results:
        require(result.get('Class')=='lang-pkgs' and result.get('Type')=='jar', 'Unexpected Trivy result type')
        for package in result.get('Packages',[]):
            packages.add(from_purl(package.get('Identifier',{}).get('PURL')))
        for vuln in result.get('Vulnerabilities',[]) or []:
            severity=vuln.get('Severity'); require(severity in {'UNKNOWN','LOW','MEDIUM','HIGH','CRITICAL'}, 'Invalid vulnerability severity')
            identity=from_purl(vuln.get('PkgIdentifier',{}).get('PURL'))
            require(identity in packages, 'Finding refers to an unreported package')
            require(vuln.get('VulnerabilityID'), 'Missing vulnerability ID')
            findings.append({'id':vuln['VulnerabilityID'],'package':purl(identity),'severity':severity,
                             'fixedVersion':vuln.get('FixedVersion','')})
    require(set(expected)<=packages, 'Scanner omitted SBOM packages: '+str(sorted(set(expected)-packages)))
    return findings

def apply_policy(findings, exceptions, today=None):
    today=today or dt.datetime.now(dt.timezone.utc).date()
    require(isinstance(exceptions,list), 'Exception list must be an array')
    keys=set(); used=set(); accepted=[]; blocked=[]
    for item in exceptions:
        require(set(item)=={'id','package','owner','reason','expires'}, 'Invalid exception fields')
        require(all(isinstance(v,str) and v.strip() for v in item.values()), 'Empty exception field')
        require(item['package']==purl(from_purl(item['package'])), 'Exception needs an exact canonical Maven package/version')
        require(dt.date.fromisoformat(item['expires'])>today, 'Expired exception: '+item['id'])
        key=(item['id'],item['package']); require(key not in keys, 'Duplicate exception'); keys.add(key)
    for finding in findings:
        if finding['severity'] not in {'HIGH','CRITICAL'}: continue
        key=(finding['id'],finding['package'])
        if key in keys: accepted.append(finding); used.add(key)
        else: blocked.append(finding)
    require(used==keys, 'Unmatched exceptions: '+str(sorted(keys-used)))
    return {'status':'FAIL' if blocked else 'PASS','blockingFindings':blocked,'exceptedFindings':accepted,
            'exceptions':exceptions,'evaluatedOn':str(today)}

def execute(args, log, timeout=600, env=None, stderr_log=None):
    with contextlib.ExitStack() as stack:
        output=stack.enter_context(Path(log).open('w'))
        errors=stack.enter_context(Path(stderr_log).open('w')) if stderr_log else subprocess.STDOUT
        result=subprocess.run(args,stdout=output,stderr=errors,timeout=timeout,env=env)
    require(result.returncode==0, 'Scanner/tool failed; see '+str(log))

def scan(root, trivy, output, exceptions_path):
    output.mkdir(parents=True,exist_ok=False)
    require(exceptions_path.resolve()==(root/'scripts/release/exceptions.json').resolve(), 'Use the reviewed checked-in exception list')
    inventory=read(root/'build/release-inputs/inventory.json')
    sboms,packages=validate_inventory(root,inventory)
    write(output/'input-check.json',{'status':'PASS','inventorySha256':sha(root/'build/release-inputs/inventory.json'),
                                    'exceptionsSha256':sha(exceptions_path),
                                    'sboms':{name:sha(root/path) for name,path in sboms.items()}})
    cache=root/'build/trivy-cache'; cache.mkdir(parents=True,exist_ok=True)
    empty=output/'empty-config.yaml'; empty.write_text('{}\n')
    ignore=output/'empty-ignore'; ignore.write_text('')
    env={k:v for k,v in os.environ.items() if not k.startswith('TRIVY_')}
    base=[str(trivy),'--cache-dir',str(cache),'--config',str(empty)]
    execute(base+['--version','--format','json'],output/'scanner.json',env=env,timeout=30,stderr_log=output/'scanner.log')
    scanner=read(output/'scanner.json')
    require(scanner.get('Version')=='0.74.0', 'Unexpected scanner version')
    write(output/'scanner-identity.json',{'version':scanner['Version'],'binarySha256':sha(trivy),
        'startedAt':dt.datetime.now(dt.timezone.utc).isoformat(),'databaseRepository':'ghcr.io/aquasecurity/trivy-db:2'})
    execute(base+['image','--download-db-only','--db-repository','ghcr.io/aquasecurity/trivy-db:2','--timeout','5m','--no-progress'],output/'database.log',env=env,timeout=330)
    db=read(cache/'db/metadata.json')
    require(db.get('Version') and db.get('UpdatedAt') and db.get('NextUpdate'), 'Missing database identity')
    require(dt.datetime.fromisoformat(db['NextUpdate'].replace('Z','+00:00'))>dt.datetime.now(dt.timezone.utc), 'Expired vulnerability database')
    write(output/'database.json',dict(db,sha256=sha(cache/'db/trivy.db')))
    fixture=root/'scripts/release/fixtures/vulnerable.cdx.json'
    fixture_report=output/'scanner-self-test.json'
    execute(base+['sbom','--skip-db-update','--skip-version-check','--scanners','vuln','--list-all-pkgs',
        '--ignorefile',str(ignore),'--ignore-unfixed=false','--format','json','--output',str(fixture_report),
        '--timeout','5m',str(fixture)],output/'scanner-self-test.log',env=env)
    known=validate_scan(read(fixture_report),{('org.apache.logging.log4j','log4j-core','2.14.1')})
    require(any(f['id']=='CVE-2021-44228' and f['severity']=='CRITICAL' for f in known), 'Scanner failed known-vulnerable fixture')
    findings=[]
    for name,path in sboms.items():
        report=output/(name+'.json')
        execute(base+['sbom','--skip-db-update','--skip-version-check','--scanners','vuln','--list-all-pkgs',
            '--ignorefile',str(ignore),'--ignore-unfixed=false','--severity','UNKNOWN,LOW,MEDIUM,HIGH,CRITICAL',
            '--format','json','--output',str(report),'--timeout','5m',str(root/path)],output/(name+'.log'),env=env)
        findings.extend(validate_scan(read(report),packages[name]))
    findings=list({(f['id'],f['package'],f['severity']):f for f in findings}.values())
    policy=apply_policy(findings,read(exceptions_path))
    write(output/'acceptance.json',dict(policy,findings=findings,scannerVersion=scanner['Version'],database=db))
    (output/'summary.txt').write_text(f"Status: {policy['status']}\nScanned SBOMs: {len(sboms)}\nFindings: {len(findings)}\nBlocking: {len(policy['blockingFindings'])}\n"+
        '\n'.join(f"{f['severity']} {f['id']} {f['package']} fixed={f['fixedVersion']}" for f in findings)+'\n')
    require(policy['status']=='PASS', 'Unexcepted HIGH/CRITICAL vulnerabilities; see acceptance.json')

def source_identity(root, ref, intended, mode):
    def git(*args): return subprocess.check_output(['git','-C',str(root),*args],text=True).strip()
    commit=git('rev-parse','HEAD'); require(git('rev-parse','--verify',ref+'^{commit}')==commit, 'Ref does not identify checkout HEAD')
    version=next(line.split('=',1)[1].strip() for line in (root/'gradle.properties').read_text().splitlines() if line.startswith('version='))
    require(intended==version, 'Intended version differs from checked-out gradle.properties')
    dirty=bool(git('status','--porcelain','--untracked-files=normal'))
    if mode=='final':
        require(not dirty, 'Final evidence needs a clean checkout')
        require(re.fullmatch(r'\d+\.\d+\.\d+',version), 'Final evidence requires a stable non-SNAPSHOT version')
        require(ref=='refs/tags/v'+version, 'Final ref must match refs/tags/v<version>')
    return {'commit':commit,'ref':ref,'version':version,'mode':mode,'dirty':dirty}

def verify_manifest(folder):
    manifest=read(folder/'manifest.json')
    require(manifest.get('schemaVersion')==1 and manifest.get('files'), 'Empty/invalid manifest')
    paths=[entry['path'] for entry in manifest['files']]
    require(len(paths)==len(set(paths)), 'Duplicate manifest path')
    require(set(paths)=={str(p.relative_to(folder)) for p in folder.rglob('*') if p.is_file() and p!=folder/'manifest.json'}, 'Manifest does not cover exact bundle file set')
    for entry in manifest['files']: require(sha(local(folder,entry['path']))==entry['sha256'], 'Bundle checksum mismatch: '+entry['path'])
    return manifest

def stage(root, scan_dir, output, ref, version, mode):
    identity=source_identity(root,ref,version,mode)
    inventory=read(root/'build/release-inputs/inventory.json'); sboms,packages=validate_inventory(root,inventory)
    require(inventory['version']==version, 'Inventory version mismatch')
    checks=read(scan_dir/'input-check.json')
    require(checks['inventorySha256']==sha(root/'build/release-inputs/inventory.json'), 'Scan is for a different inventory')
    require(checks['sboms']=={name:sha(root/path) for name,path in sboms.items()}, 'Scan is for different SBOM bytes')
    acceptance=read(scan_dir/'acceptance.json')
    require(acceptance['status']=='PASS','Rejected scan cannot become accepted evidence')
    require(checks['exceptionsSha256']==sha(root/'scripts/release/exceptions.json'), 'Exception list changed after scan')
    require(acceptance['exceptions']==read(root/'scripts/release/exceptions.json'), 'Unreviewed exceptions in scan report')
    findings=[]
    for name in sboms: findings.extend(validate_scan(read(scan_dir/(name+'.json')),packages[name]))
    findings=list({(f['id'],f['package'],f['severity']):f for f in findings}.values())
    require(apply_policy(findings,acceptance['exceptions'])['status']=='PASS', 'Scan policy no longer passes')
    require(not (scan_dir/'failure.json').exists(), 'Failed scan cannot become accepted evidence')
    require(read(scan_dir/'scanner.json').get('Version')=='0.74.0', 'Missing scanner identity')
    database=read(scan_dir/'database.json')
    require(database.get('sha256') and database.get('UpdatedAt'), 'Missing database evidence')
    require(dt.datetime.fromisoformat(database['NextUpdate'].replace('Z','+00:00'))>dt.datetime.now(dt.timezone.utc), 'Database expired before staging')
    output.mkdir(parents=True,exist_ok=False); entries=[]
    def copy(source, relative, coord=None):
        dest=output/relative; dest.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(source,dest)
        entries.append({'path':str(relative),'sha256':sha(dest),'coordinate':coord})
    for module in inventory['modules']:
        for pub in module['publications']:
            for item in pub['files']:
                suffix='-'+item['classifier'] if item['classifier'] else ''
                filename=f"{pub['name']}-{version}{suffix}.{item['extension']}"
                coord=dict(group=pub['group'],name=pub['name'],version=version,classifier=item['classifier'],extension=item['extension'])
                copy(local(root,item['path']),Path('artifacts')/filename,coord)
    for name,path in sboms.items(): copy(root/path,Path('sbom')/(name+'.json'))
    copy(root/'build/release-inputs/inventory.json',Path('inventory.json'))
    for path in sorted(scan_dir.iterdir()):
        if path.is_file() and path.suffix in {'.json','.txt','.log'}: copy(path,Path('scan')/path.name)
    write(output/'manifest.json',dict(schemaVersion=1,source=identity,files=entries))
    verify_manifest(output)

def main():
    parser=argparse.ArgumentParser(); sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('scan'); p.add_argument('--root',type=Path,default=Path.cwd()); p.add_argument('--trivy',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True); p.add_argument('--exceptions',type=Path,required=True)
    p=sub.add_parser('stage'); p.add_argument('--root',type=Path,default=Path.cwd()); p.add_argument('--scan',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True); p.add_argument('--ref',required=True); p.add_argument('--version',required=True)
    p.add_argument('--mode',choices=['trial','final'],default='trial')
    p=sub.add_parser('verify'); p.add_argument('folder',type=Path)
    p=sub.add_parser('identity'); p.add_argument('--ref',required=True); p.add_argument('--version',required=True)
    p.add_argument('--mode',choices=['trial','final'],required=True)
    p=sub.add_parser('compare-published'); p.add_argument('folder',type=Path); p.add_argument('published',type=Path)
    args=parser.parse_args()
    try:
        if args.command=='scan': scan(args.root.resolve(),args.trivy.resolve(),args.output.resolve(),args.exceptions)
        elif args.command=='stage': stage(args.root.resolve(),args.scan.resolve(),args.output.resolve(),args.ref,args.version,args.mode)
        elif args.command=='identity': print(json.dumps(source_identity(Path.cwd(),args.ref,args.version,args.mode)))
        elif args.command=='compare-published':
            manifest=verify_manifest(args.folder.resolve())
            for entry in manifest['files']:
                if entry.get('coordinate'):
                    artifact=local(args.published,Path(entry['path']).name)
                    require(sha(artifact)==entry['sha256'], 'Published artifact differs: '+artifact.name)
            print('All published artifact bytes match the candidate manifest')
        else: verify_manifest(args.folder.resolve())
    except (EvidenceError,ValueError,KeyError,OSError,subprocess.SubprocessError) as error:
        if args.command=='scan':
            args.output.mkdir(parents=True,exist_ok=True)
            write(args.output/'failure.json',{'status':'FAIL','error':str(error)})
        print(str(error),file=sys.stderr); return 1
    return 0
if __name__=='__main__': sys.exit(main())
