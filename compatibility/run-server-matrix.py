#!/usr/bin/env python3
"""Run identical transport contracts, retaining reports for every server profile."""
import json, shutil, subprocess, sys
from pathlib import Path
from fixture_config import ROOT, PROFILES as ALL_PROFILES
PROFILES={k:ALL_PROFILES[k] for k in ("redis62", "redis74", "redis8", "valkey")}
for profile in sys.argv[1:] or PROFILES:
    image=PROFILES[profile]
    output=ROOT/"build/compatibility-evidence"/profile;output.mkdir(parents=True,exist_ok=True)
    args=[str(ROOT/"gradlew"), ":tiercache-transport-redis:serverContractTest", "--max-workers=2", "-PserverImage="+image]
    reports=ROOT/"tiercache-transport-redis/build/test-results/serverContractTest"
    if reports.exists(): shutil.rmtree(reports)
    if (output/"test-results").exists(): shutil.rmtree(output/"test-results")
    with (output/"gradle.log").open("w") as log:
        result=subprocess.run(args,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,timeout=1200)
    if reports.exists(): shutil.copytree(reports,output/"test-results",dirs_exist_ok=True)
    inspection=subprocess.run(["docker","image","inspect",image],capture_output=True,text=True,timeout=30)
    (output/"image.json").write_text(inspection.stdout or inspection.stderr)
    (output/"run.json").write_text(json.dumps({"command":args,"exitCode":result.returncode,"image":image},indent=2))
    result.check_returncode()
