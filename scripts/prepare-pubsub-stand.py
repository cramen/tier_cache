#!/usr/bin/env python3
"""Prepare an isolated Docker build directory from an exported candidate test runtime."""
import argparse
from pathlib import Path
import os
import shutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--classpath-file', type=Path, required=True)
parser.add_argument('--output', type=Path, default=Path('build/pubsub-stand'))
args = parser.parse_args()
recipe = Path(__file__).resolve().parent / 'pubsub-stand'
args.output.mkdir(parents=True, exist_ok=True)
for name in ['Dockerfile', 'compose.yaml', 'origin.py', 'nginx.conf', 'run.py']:
    shutil.copy2(recipe / name, args.output / name)
for name in ['classes', 'lib']:
    folder = args.output / name
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir()
for index, raw in enumerate(args.classpath_file.read_text().strip().split(os.pathsep)):
    source = Path(raw)
    if source.is_dir():
        shutil.copytree(source, args.output / 'classes', dirs_exist_ok=True)
    elif source.is_file():
        shutil.copy2(source, args.output / 'lib' / f'{index:03}-{source.name}')
print(args.output.resolve())
