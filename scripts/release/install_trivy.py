#!/usr/bin/env python3
"""Install the pinned scanner, verifying a checked-in release archive checksum."""
import hashlib, io, platform, sys, tarfile, urllib.request
from pathlib import Path
VERSION = '0.74.0'
ARCHIVES = {
    ('Linux', 'x86_64'): ('Linux-64bit', '2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a'),
    ('Darwin', 'arm64'): ('macOS-ARM64', '1caada5e0e2091909357c7525d3aa76f4b660b13821bc143b190c7483e31cc11'),
}
def main():
    target = Path(sys.argv[1]).resolve(); target.mkdir(parents=True, exist_ok=True)
    suffix, expected = ARCHIVES[(platform.system(), platform.machine())]
    url = f'https://github.com/aquasecurity/trivy/releases/download/v{VERSION}/trivy_{VERSION}_{suffix}.tar.gz'
    data = urllib.request.urlopen(url, timeout=120).read()
    if hashlib.sha256(data).hexdigest() != expected: raise RuntimeError('Trivy archive checksum mismatch')
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        executable = archive.extractfile('trivy')
        if executable is None: raise RuntimeError('Missing scanner executable')
        binary = target/'trivy'; binary.write_bytes(executable.read()); binary.chmod(0o755)
    print(binary)
if __name__ == '__main__': main()
