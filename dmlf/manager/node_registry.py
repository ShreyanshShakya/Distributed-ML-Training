import sqlite3
import json
import time
from typing import List, Dict, Any

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
                    last_heartbeat REAL
                )
            ''')
            # Metrics table (optional tracking)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    cpu_percent REAL,
                    ram_percent REAL,
                    gpu_utilization REAL,
                    gpu_memory_mb REAL,
                    FOREIGN KEY(node_id) REFERENCES nodes(node_id)
                )
            ''')
            conn.commit()

    def register_node(self, node_id: str, hostname: str, ip_address: str, cpu_count: int, gpu_model: str, ram_total: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO nodes (node_id, hostname, ip_address, cpu_count, gpu_model, ram_total, status, last_heartbeat)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    hostname=excluded.hostname,
                    ip_address=excluded.ip_address,
                    cpu_count=excluded.cpu_count,
                    gpu_model=excluded.gpu_model,
                    ram_total=excluded.ram_total,
                    status=excluded.status,
                    last_heartbeat=excluded.last_heartbeat
            ''', (node_id, hostname, ip_address, cpu_count, gpu_model, ram_total, 'idle', time.time()))
            conn.commit()
        return True

    def update_heartbeat(self, node_id: str, status: str, metrics: Dict[str, Any]):
        timestamp = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Update last heartbeat and status
            cursor.execute('''
                UPDATE nodes SET last_heartbeat = ?, status = ? WHERE node_id = ?
            ''', (timestamp, status, node_id))
            
            # Insert metrics
            cursor.execute('''
                INSERT INTO metrics (node_id, timestamp, cpu_percent, ram_percent, gpu_utilization, gpu_memory_mb)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (node_id, timestamp, metrics.get("cpu_percent", 0), metrics.get("ram_percent", 0), 
                  metrics.get("gpu_utilization", 0), metrics.get("gpu_memory_mb", 0)))
            conn.commit()

    def mark_offline_nodes(self, timeout_seconds: float = 15.0):
        """Marks nodes as offline if they haven't sent a heartbeat within the timeout."""
        cutoff_time = time.time() - timeout_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE nodes SET status = 'offline' WHERE last_heartbeat < ? AND status != 'offline'
            ''', (cutoff_time,))
            conn.commit()

    def get_available_nodes(self) -> List[Dict[str, Any]]:
        self.mark_offline_nodes()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE status = 'idle'")
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
