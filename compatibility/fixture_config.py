from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
PROFILES = dict(line.split('=', 1) for line in (ROOT/'compatibility/platforms.properties').read_text().splitlines()
                if line and not line.startswith('#'))
