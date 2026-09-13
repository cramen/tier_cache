#!/usr/bin/env python3
"""Load driver for the 8-instance cluster soak: uniform reads over a bounded
key space so L1 (10 s) and L2 (60 s) expire many times during the run.
Writes per-request latency/status as CSV lines to stdout."""
import random
import sys
import threading
import time
import urllib.request

BASE = "http://localhost:8080/greeting/k"
KEYS = 150
DURATION_S = float(sys.argv[1]) if len(sys.argv) > 1 else 360
WORKERS = 8

stop_at = time.time() + DURATION_S
lock = threading.Lock()


def worker(wid):
    rnd = random.Random(wid)
    while time.time() < stop_at:
        key = rnd.randrange(KEYS)
        t0 = time.time()
        try:
            with urllib.request.urlopen(f"{BASE}{key}", timeout=30) as r:
                r.read()
                code = r.status
        except Exception as e:
            code = -1
        ms = int((time.time() - t0) * 1000)
        with lock:
            print(f"{int(t0)},{code},{ms}", flush=True)


threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(WORKERS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
