import sqlite3
import json
import time
from typing import List, Dict, Any, Optional

class NodeState:
    REGISTERING = "REGISTERING"
    IDLE = "IDLE"
    RESERVED = "RESERVED"
    TRAINING = "TRAINING"
    BUSY = "BUSY"
    MAINTENANCE = "MAINTENANCE"
    DISCONNECTED = "DISCONNECTED"
    FAILED = "FAILED"

class JobState:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class NodeRegistry:
    def __init__(self, db_path: str = "cluster.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Nodes table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    hostname TEXT,
                    ip_address TEXT,
                    cpu_count INTEGER,
                    gpu_model TEXT,
                    ram_total TEXT,
                    status TEXT,
                    last_heartbeat REAL,
                    node_secret TEXT
                )
            ''')
            # Ensure node_secret column exists for older DBs
            try:
                cursor.execute("ALTER TABLE nodes ADD COLUMN node_secret TEXT")
            except sqlite3.OperationalError:
                pass
            # Metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    cpu_percent REAL,
                    ram_percent REAL,
                    gpu_utilization REAL,
                    gpu_memory_mb REAL,
                    heartbeat_latency_ms REAL,
                    FOREIGN KEY(node_id) REFERENCES nodes(node_id)
                )
            ''')
            # Jobs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_name TEXT,
                    status TEXT,
                    script_path TEXT,
                    config_path TEXT,
                    required_nodes INTEGER,
                    assigned_nodes TEXT,
                    priority INTEGER,
                    created_at REAL,
                    started_at REAL,
                    completed_at REAL,
                    exit_code INTEGER,
                    experiment_id TEXT,
                    nproc_per_node INTEGER,
                    args TEXT,
                    retries INTEGER,
                    max_retries INTEGER
                )
            ''')
            conn.commit()

    def register_node(self, node_id: str, hostname: str, ip_address: str, cpu_count: int, gpu_model: str, ram_total: str, node_secret: str = None) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO nodes (node_id, hostname, ip_address, cpu_count, gpu_model, ram_total, status, last_heartbeat, node_secret)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    hostname=excluded.hostname,
                    ip_address=excluded.ip_address,
                    cpu_count=excluded.cpu_count,
                    gpu_model=excluded.gpu_model,
                    ram_total=excluded.ram_total,
                    status=excluded.status,
                    last_heartbeat=excluded.last_heartbeat,
                    node_secret=excluded.node_secret
            ''', (node_id, hostname, ip_address, cpu_count, gpu_model, ram_total, NodeState.IDLE, time.time(), node_secret))
            conn.commit()
        return True

    def update_heartbeat(self, node_id: str, status: str, metrics: Dict[str, Any], latency_ms: float = 0.0):
        timestamp = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE nodes SET last_heartbeat = ?, status = ? WHERE node_id = ?
            ''', (timestamp, status, node_id))
            
            cursor.execute('''
                INSERT INTO metrics (node_id, timestamp, cpu_percent, ram_percent, gpu_utilization, gpu_memory_mb, heartbeat_latency_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (node_id, timestamp, metrics.get("cpu_percent", 0), metrics.get("ram_percent", 0), 
                  metrics.get("gpu_utilization", 0), metrics.get("gpu_memory_mb", 0), latency_ms))
            conn.commit()

    def mark_offline_nodes(self, timeout_seconds: float = 15.0):
        cutoff_time = time.time() - timeout_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE nodes SET status = ? WHERE last_heartbeat < ? AND status NOT IN (?, ?)
            ''', (NodeState.DISCONNECTED, cutoff_time, NodeState.DISCONNECTED, NodeState.MAINTENANCE))
            conn.commit()

    def get_available_nodes(self) -> List[Dict[str, Any]]:
        self.mark_offline_nodes()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE status = ?", (NodeState.IDLE,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        self.mark_offline_nodes()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
    def update_node_state(self, node_id: str, new_state: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE nodes SET status = ? WHERE node_id = ?", (new_state, node_id))
            conn.commit()
            
    # --- Job Methods ---
    def insert_job(self, job_data: Dict[str, Any]):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO jobs (job_id, job_name, status, script_path, config_path, required_nodes, assigned_nodes, priority, created_at, experiment_id, nproc_per_node, args, retries, max_retries)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data["job_id"], job_data.get("job_name", "unnamed"), job_data["status"],
                job_data["script_path"], job_data.get("config_path", ""), job_data["required_nodes"],
                json.dumps(job_data.get("assigned_nodes", [])), job_data.get("priority", 0),
                time.time(), job_data.get("experiment_id", ""), job_data.get("nproc_per_node", 1), 
                job_data.get("args", ""), job_data.get("retries", 0), job_data.get("max_retries", 3)
            ))
            conn.commit()
            
    def update_job_status(self, job_id: str, status: str, assigned_nodes: Optional[List[str]] = None, exit_code: Optional[int] = None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            updates = ["status = ?"]
            params = [status]
            
            if status == JobState.RUNNING:
                updates.append("started_at = ?")
                params.append(time.time())
            elif status in (JobState.COMPLETED, JobState.FAILED):
                updates.append("completed_at = ?")
                params.append(time.time())
                
            if assigned_nodes is not None:
                updates.append("assigned_nodes = ?")
                params.append(json.dumps(assigned_nodes))
                
            if exit_code is not None:
                updates.append("exit_code = ?")
                params.append(exit_code)
                
            params.append(job_id)
            query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"
            cursor.execute(query, tuple(params))
            conn.commit()
            
    def update_job_retries(self, job_id: str, new_status: str, retries: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE jobs SET status = ?, retries = ? WHERE job_id = ?", (new_status, retries, job_id))
            conn.commit()
            
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
            
    def get_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT * FROM jobs WHERE status = ?", (status,))
            else:
                cursor.execute("SELECT * FROM jobs")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_node_secret(self, node_id: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT node_secret FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            return row["node_secret"] if row else None
