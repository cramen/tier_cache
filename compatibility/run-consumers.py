#!/usr/bin/env python3
"""Run isolated Boot consumer graphs against one ephemeral standalone Redis."""
import json, subprocess, time, uuid
from pathlib import Path
from fixture_config import ROOT, PROFILES
name = "tiercache-consumer-" + uuid.uuid4().hex[:12]
version = next(x.split("=",1)[1].strip() for x in (ROOT/"gradle.properties").read_text().splitlines() if x.startswith("version="))
def run(args, **kw): return subprocess.run(args, check=True, timeout=900, **kw)
try:
    run(["docker", "run", "-d", "--name", name, "-p", "127.0.0.1::6379", PROFILES["redis62"]])
    port = subprocess.check_output(["docker", "port", name, "6379"], text=True).strip().rsplit(":",1)[1]
    deadline=time.monotonic()+30
    while subprocess.run(["docker","exec",name,"redis-cli","PING"],capture_output=True,text=True,timeout=5).stdout.strip()!="PONG":
        if time.monotonic()>deadline: raise RuntimeError("Consumer Redis readiness timed out")
        time.sleep(.1)
    for boot in [PROFILES["boot35"], PROFILES["boot4"]]:
        args=[str(ROOT/"gradlew"), "-p", str(ROOT/"compatibility/spring-consumer"), "clean", "test", "--max-workers=2",
             "-PbootVersion="+boot, "-PtiercacheVersion="+version,
             "-PartifactRepository="+str(ROOT/"build/compatibility-repository"), "-PredisUri=redis://127.0.0.1:"+port]
        result=subprocess.run(args, timeout=900)
        import shutil
        target=ROOT/"build/compatibility-evidence"/("boot-"+boot)
        if target.exists(): shutil.rmtree(target)
        shutil.copytree(ROOT/"compatibility/spring-consumer/build", target)
        (target/"command.json").write_text(json.dumps({"command":args,"exitCode":result.returncode},indent=2))
        (target/"redis-image.json").write_text(subprocess.check_output(["docker","image","inspect",PROFILES["redis62"]],text=True,timeout=30))
        result.check_returncode()
finally:
    subprocess.run(["docker", "rm", "-f", "-v", name], timeout=30, check=False)
