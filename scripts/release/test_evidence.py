import copy
import datetime as dt
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
import evidence as e

CAFFEINE=('com.github.ben-manes.caffeine','caffeine','3.2.0')
SLF4J=('org.slf4j','slf4j-api','2.0.16')
LETTUCE=('io.lettuce','lettuce-core','7.7.0.RELEASE')

def component(coord):
    return dict(zip(('group','name','version'),coord),purl=e.purl(coord))
def sbom(identity, components):
    return {'bomFormat':'CycloneDX','metadata':{'component':component(identity)},'components':[component(c) for c in components]}
def report(packages, findings=()):
    return {'SchemaVersion':2,'ArtifactType':'cyclonedx','Results':[{'Class':'lang-pkgs','Type':'jar',
        'Packages':[{'Identifier':{'PURL':e.purl(p)}} for p in packages],
        'Vulnerabilities':list(findings)}]}
def finding(severity='HIGH'):
    return {'VulnerabilityID':'CVE-2099-0001','PkgIdentifier':{'PURL':e.purl(CAFFEINE)},'Severity':severity,'FixedVersion':'9.9'}

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
    def inventory(self):
        inv={'schemaVersion':1,'group':e.GROUP,'name':'tiercache','version':'2.0.0','modules':[], 'aggregateSbom':'aggregate.json'}
        all_deps={CAFFEINE,SLF4J,LETTUCE}
        for name in sorted(e.MODULES):
            deps={SLF4J}
            if name=='tiercache-core':deps.add(CAFFEINE)
            if name=='tiercache-transport-redis':deps.add(LETTUCE)
            configs={'runtimeClasspath':[component(c) for c in deps]}
            if name=='tiercache-core':configs['testFixturesRuntimeClasspath']=[component(SLF4J)]
            if name=='tiercache-tck':configs['testRuntimeClasspath']=[component(SLF4J)]
            classifiers={'','sources','javadoc'}
            if name=='tiercache-core':classifiers|={'unshaded','test-fixtures','test-fixtures-sources'}
            if name=='tiercache-tck':classifiers.add('tests')
            files=[]; module_files=[]
            for classifier in sorted(classifiers):
                filename=name+('-'+classifier if classifier else '')+'.jar'; path=self.root/filename
                with zipfile.ZipFile(path,'w') as jar: jar.writestr('io/tiercache/internal/caffeine/cache/Cache.class',b'test')
                files.append(dict(path=filename,classifier=classifier,extension='jar',sha256=e.sha(path)))
                module_files.append(dict(name=filename,sha256=e.sha(path)))
            pom=self.root/(name+'.pom');pom.write_text(f'<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>{e.GROUP}</groupId><artifactId>{name}</artifactId><version>2.0.0</version></project>')
            metadata=self.root/(name+'.module');e.write(metadata,{'component':{'group':e.GROUP,'module':name,'version':'2.0.0'},'variants':[{'files':module_files}]})
            for path,ext in [(pom,'pom'),(metadata,'module')]:files.append(dict(path=path.name,classifier='',extension=ext,sha256=e.sha(path)))
            e.write(self.root/(name+'.json'),sbom((e.GROUP,name,'2.0.0'),deps))
            inv['modules'].append(dict(name=name,configurations=configs,sbom=name+'.json',
                publications=[dict(group=e.GROUP,name=name,version='2.0.0',files=files)]))
        e.write(self.root/'aggregate.json',sbom((e.GROUP,'tiercache','2.0.0'),all_deps|{(e.GROUP,n,'2.0.0') for n in e.MODULES}))
        return inv
    def test_complete_realistic_publication_inventory(self):
        docs,packages=e.validate_inventory(self.root,self.inventory());self.assertEqual(10,len(docs));self.assertIn(CAFFEINE,packages['tiercache-core'])
    def test_missing_fixture_classifier_is_rejected(self):
        inv=self.inventory();core=next(m for m in inv['modules'] if m['name']=='tiercache-core')
        core['publications'][0]['files']=[f for f in core['publications'][0]['files'] if f['classifier']!='test-fixtures']
        with self.assertRaises(e.EvidenceError):e.validate_inventory(self.root,inv)
    def test_missing_classifier_runtime_graph_is_rejected(self):
        inv=self.inventory();next(m for m in inv['modules'] if m['name']=='tiercache-tck')['configurations'].pop('testRuntimeClasspath')
        with self.assertRaises(e.EvidenceError):e.validate_inventory(self.root,inv)
    def test_missing_shaded_dependency_in_sbom(self):
        inv=self.inventory();path=self.root/'tiercache-core.json';doc=e.read(path);doc['components']=[component(SLF4J)];e.write(path,doc)
        with self.assertRaisesRegex(e.EvidenceError,'omits resolved'):e.validate_inventory(self.root,inv)
    def test_missing_shaded_dependency_in_inventory_also_fails(self):
        inv=self.inventory();core=next(m for m in inv['modules'] if m['name']=='tiercache-core')
        core['configurations']['runtimeClasspath']=[component(SLF4J)]
        with self.assertRaisesRegex(e.EvidenceError,'Caffeine'):e.validate_inventory(self.root,inv)
    def test_missing_module_fails(self):
        inv=self.inventory();inv['modules'].pop()
        with self.assertRaises(e.EvidenceError):e.validate_inventory(self.root,inv)
    def test_empty_component_list_fails(self):
        with self.assertRaises(e.EvidenceError):e.validate_sbom(sbom(('x','y','1'),[]),{SLF4J},('x','y','1'))
    def test_scanner_coverage_cannot_be_replaced_by_clean_status(self):
        with self.assertRaises(e.EvidenceError):e.validate_scan(report([SLF4J]),{SLF4J,CAFFEINE})
    def test_unparsed_report_fails(self):
        with self.assertRaises(e.EvidenceError):e.validate_scan({'SchemaVersion':2,'Results':[]},{SLF4J})
    def test_clean_scan_passes(self):
        findings=e.validate_scan(report([CAFFEINE]),{CAFFEINE});self.assertEqual('PASS',e.apply_policy(findings,[])['status'])
    def test_known_vulnerable_synthetic_fixture_blocks(self):
        findings=e.validate_scan(report([CAFFEINE],[finding()]),{CAFFEINE});self.assertEqual('FAIL',e.apply_policy(findings,[])['status'])
    def exception(self, expires='2100-01-01'):
        return dict(id='CVE-2099-0001',package=e.purl(CAFFEINE),owner='security-owner',reason='Synthetic scoped test only',expires=expires)
    def test_scoped_exception_allows_only_exact_finding(self):
        findings=e.validate_scan(report([CAFFEINE],[finding()]),{CAFFEINE})
        self.assertEqual('PASS',e.apply_policy(findings,[self.exception()])['status'])
        wrong=self.exception();wrong['package']=e.purl(('other','artifact','1'))
        with self.assertRaises(e.EvidenceError):e.apply_policy(findings,[wrong])
    def test_expired_and_unmatched_exceptions_fail(self):
        for exceptions in [[self.exception('2000-01-01')],[self.exception()]]:
            with self.assertRaises(e.EvidenceError):e.apply_policy([],exceptions)
    def test_exception_expiring_today_fails(self):
        with self.assertRaises(e.EvidenceError):e.apply_policy([],[self.exception('2026-09-22')],dt.date(2026,9,22))
    def test_json_stdout_stays_parseable_when_scanner_logs_to_stderr(self):
        e.execute([sys.executable,'-c', 'import sys,json; print("diagnostic",file=sys.stderr); print(json.dumps(dict(Version="0.74.0")))'],
            self.root/'version.json',stderr_log=self.root/'version.log')
        self.assertEqual('0.74.0',e.read(self.root/'version.json')['Version'])
        self.assertIn('diagnostic',(self.root/'version.log').read_text())
    def test_scanner_process_failure_fails(self):
        with self.assertRaises(e.EvidenceError):e.execute([sys.executable,'-c','raise SystemExit(2)'],self.root/'scanner.log')
    def test_corrupted_input_artifact_fails(self):
        inv=self.inventory();file=inv['modules'][0]['publications'][0]['files'][0];(self.root/file['path']).write_bytes(b'changed')
        with self.assertRaisesRegex(e.EvidenceError,'checksum'):e.validate_inventory(self.root,inv)
    def test_manifest_detects_changed_bytes_and_extras(self):
        jar=self.root/'artifact.jar';jar.write_bytes(b'original')
        e.write(self.root/'manifest.json',{'schemaVersion':1,'files':[{'path':'artifact.jar','sha256':e.sha(jar)}]})
        e.verify_manifest(self.root)
        jar.write_bytes(b'changed')
        with self.assertRaises(e.EvidenceError):e.verify_manifest(self.root)
        jar.write_bytes(b'original');(self.root/'leftover.jar').write_bytes(b'old')
        with self.assertRaises(e.EvidenceError):e.verify_manifest(self.root)
    def prepared_scan(self):
        inv=self.inventory();e.write(self.root/'build/release-inputs/inventory.json',inv)
        e.write(self.root/'scripts/release/exceptions.json',[])
        docs,packages=e.validate_inventory(self.root,inv)
        scans=self.root/'scan';scans.mkdir()
        for name,pkgs in packages.items():e.write(scans/(name+'.json'),report(pkgs))
        e.write(scans/'input-check.json',{'inventorySha256':e.sha(self.root/'build/release-inputs/inventory.json'),
            'exceptionsSha256':e.sha(self.root/'scripts/release/exceptions.json'),
            'sboms':{name:e.sha(self.root/path) for name,path in docs.items()}})
        e.write(scans/'acceptance.json',{'status':'PASS','exceptions':[]})
        e.write(scans/'scanner.json',{'Version':'0.74.0'})
        e.write(scans/'database.json',{'UpdatedAt':'2026-01-01T00:00:00Z','NextUpdate':'2100-01-01T00:00:00Z','sha256':'a'*64})
        return scans
    def test_stage_covers_fixtures_and_metadata_and_rejects_reuse(self):
        scans=self.prepared_scan();output=self.root/'dist'
        with patch('evidence.source_identity',return_value={'commit':'a'*40,'version':'2.0.0','mode':'trial'}):
            e.stage(self.root,scans,output,'a'*40,'2.0.0','trial')
            paths={f['path'] for f in e.verify_manifest(output)['files']}
            self.assertIn('artifacts/tiercache-core-2.0.0-test-fixtures.jar',paths)
            self.assertIn('artifacts/tiercache-tck-2.0.0-tests.jar',paths)
            self.assertIn('artifacts/tiercache-core-2.0.0.module',paths)
            with self.assertRaises(FileExistsError):e.stage(self.root,scans,output,'a'*40,'2.0.0','trial')
    def test_stage_rechecks_scanner_coverage_not_just_pass_label(self):
        scans=self.prepared_scan();e.write(scans/'tiercache-core.json',report([SLF4J]))
        with patch('evidence.source_identity',return_value={}):
            with self.assertRaises(e.EvidenceError):e.stage(self.root,scans,self.root/'dist','a'*40,'2.0.0','trial')
    def test_stage_rejects_changed_sbom_after_scan(self):
        scans=self.prepared_scan();path=self.root/'aggregate.json';path.write_text(path.read_text()+' ')
        with patch('evidence.source_identity',return_value={}):
            with self.assertRaises(e.EvidenceError):e.stage(self.root,scans,self.root/'dist','a'*40,'2.0.0','trial')
    def test_nested_manifest_is_not_exempt_from_checksums(self):
        folder=self.root/'nested';folder.mkdir();(folder/'manifest.json').write_text('{}')
        e.write(self.root/'manifest.json',{'schemaVersion':1,'files':[{'path':'nested/manifest.json','sha256':e.sha(folder/'manifest.json')}]})
        e.verify_manifest(self.root);(folder/'manifest.json').write_text('changed')
        with self.assertRaises(e.EvidenceError):e.verify_manifest(self.root)
    def test_snapshot_trial_allowed_but_final_rejected(self):
        (self.root/'gradle.properties').write_text('version=1.5.0-SNAPSHOT\n')
        with patch('evidence.subprocess.check_output',side_effect=['a'*40,'a'*40,'']):
            self.assertEqual('trial',e.source_identity(self.root,'a'*40,'1.5.0-SNAPSHOT','trial')['mode'])
        with patch('evidence.subprocess.check_output',side_effect=['a'*40,'a'*40,'']):
            with self.assertRaises(e.EvidenceError):e.source_identity(self.root,'a'*40,'1.5.0-SNAPSHOT','final')
    def test_final_version_tag_and_clean_tree_required(self):
        (self.root/'gradle.properties').write_text('version=2.0.0\n')
        for ref,dirty in [('main',''),('refs/tags/v2.0.0',' M file')]:
            with patch('evidence.subprocess.check_output',side_effect=['a'*40,'a'*40,dirty]):
                with self.assertRaises(e.EvidenceError):e.source_identity(self.root,ref,'2.0.0','final')
        with patch('evidence.subprocess.check_output',side_effect=['a'*40,'a'*40,'']):
            self.assertEqual('final',e.source_identity(self.root,'refs/tags/v2.0.0','2.0.0','final')['mode'])

if __name__=='__main__': unittest.main()
