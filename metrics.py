import sqlite3

def calc_metrics():
    with sqlite3.connect('cluster.db') as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        print('=== JOB METRICS ===')
        c.execute("SELECT job_id, created_at, started_at, completed_at FROM jobs WHERE status='COMPLETED'")
        rows = c.fetchall()
        if not rows:
            print("No completed jobs found.")
        for r in rows:
            wait_time = r['started_at'] - r['created_at']
            run_time = r['completed_at'] - r['started_at']
            print(f"Job {r['job_id']}: Wait Time = {wait_time:.1f}s, Run Time = {run_time:.1f}s")
            
        print('\n=== HARDWARE METRICS ===')
        c.execute('SELECT node_id, AVG(cpu_percent) as c, AVG(ram_percent) as r, AVG(gpu_utilization) as g, AVG(heartbeat_latency_ms) as l FROM metrics GROUP BY node_id')
        rows = c.fetchall()
        if not rows:
            print("No hardware metrics found.")
        for r in rows:
            print(f"Node {r['node_id']}: CPU={r['c']:.1f}% | RAM={r['r']:.1f}% | GPU={r['g']:.1f}% | Latency={r['l']:.1f}ms")

if __name__ == "__main__":
    calc_metrics()
