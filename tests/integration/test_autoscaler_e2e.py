#!/usr/bin/env python3
"""
End‑to‑end sanity check for the autoscaler.
Starts:
  • Cluster Manager (with autoscaler enabled)
  • 2 Node Agents
Submits 7 jobs (queue_depth_high = 5) so the autoscaler should request a scale‑out.
"""
import os
import sys
import time
import subprocess
import signal
import urllib.request
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]   # repository root
CFG = ROOT / "config.yaml"

def wait_health(url, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False

def main():
    # ------------------------------------------------------------------
    # 1️⃣  Launch Manager
    # ------------------------------------------------------------------
    mgr_cmd = [
        sys.executable, "-m", "dmlf.manager.cluster_manager",
        "--config", str(CFG)
    ]
    mgr = subprocess.Popen(mgr_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    print(f"[test] Manager PID={mgr.pid}")

    # Thread to consume manager stdout so the process never blocks on a full pipe
    mgr_log_lines = []
    def _log_reader():
        for line in mgr.stdout:
            print(f"[mgr] {line.rstrip()}")
            mgr_log_lines.append(line)
    import threading
    log_thread = threading.Thread(target=_log_reader, daemon=True)
    log_thread.start()

    # wait for health endpoint
    assert wait_health("http://localhost:8080/health"), "Manager health never came up"
    print("[test] Manager health OK")

    # ------------------------------------------------------------------
    # 2️⃣  Launch two agents
    # ------------------------------------------------------------------
    agents = []
    for i in range(2):
        ag_cmd = [
            sys.executable, "-m", "dmlf.agent.agent",
            "--config", str(CFG)
        ]
        ag = subprocess.Popen(ag_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        agents.append(ag)
        print(f"[test] Agent{i} PID={ag.pid}")

    # give agents time to register & start heartbeating
    time.sleep(5)

    # ------------------------------------------------------------------
    # 3️⃣  Submit 7 jobs (queue_depth_high = 5)
    # ------------------------------------------------------------------
    job_yaml = ROOT / "dmlf" / "configs" / "resnet.yaml"
    for i in range(7):
        cli_cmd = [
            sys.executable, "-m", "dmlf.cli",
            "--config", str(CFG),
            "submit", str(job_yaml)
        ]
        res = subprocess.run(cli_cmd, capture_output=True, text=True, timeout=30)
        print(f"[test] Submit {i}: {res.stdout.strip()}")
        if res.returncode != 0:
            print(f"[test] Submit failed: {res.stderr}")

    # ------------------------------------------------------------------
    # 4️⃣  Give autoscaler a couple of evaluation cycles (interval=30s default)
    #     Reduce interval in config.yaml for faster test (autoscaler.interval_sec: 5)
    # ------------------------------------------------------------------
    print("[test] Waiting for autoscaler evaluation (~ interval_sec * 2)...")
    time.sleep(12)   # enough for two cycles if interval_sec=5

    # ------------------------------------------------------------------
    # 5️⃣  Check manager logs for autoscaler messages
    # ------------------------------------------------------------------
    mgr_output = "".join(mgr_log_lines)
    print("=== Manager stdout (tail) ===")
    print(mgr_output[-2000:])  # last 2k chars

    # ------------------------------------------------------------------
    # 6️⃣  Clean shutdown
    # ------------------------------------------------------------------
    for ag in agents:
        ag.terminate()
        ag.wait(timeout=5)
    mgr.terminate()
    mgr.wait(timeout=5)

    # ------------------------------------------------------------------
    # 7️⃣  Verify autoscaler requested scale‑out
    # ------------------------------------------------------------------
    if "Scale‑OUT requested" in mgr_output or "Scale-OUT requested" in mgr_output:
        print("\n[OK] Autoscaler correctly emitted a scale-out request")
        sys.exit(0)
    else:
        print("\n[FAIL] No scale-out message found in manager logs")
        sys.exit(1)

if __name__ == "__main__":
    main()