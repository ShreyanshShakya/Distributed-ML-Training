import time
import json
import sqlite3
import os
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from rich.console import Group

class ClusterDashboard:
    def __init__(self, db_path="cluster.db"):
        self.db_path = db_path
        
    def generate_node_table(self) -> Table:
        table = Table(title="Cluster Status", show_lines=True)
        table.add_column("Node ID", style="cyan")
        table.add_column("IP Address", style="magenta")
        table.add_column("State", style="bold")
        table.add_column("CPU %", justify="right")
        table.add_column("RAM %", justify="right")
        table.add_column("GPU %", justify="right")
        table.add_column("Latency (ms)", justify="right")

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                # Get latest metrics for each node
                cursor.execute("""
                    SELECT n.node_id, n.ip_address, n.status, 
                           m.cpu_percent, m.ram_percent, m.gpu_utilization, m.heartbeat_latency_ms
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, cpu_percent, ram_percent, gpu_utilization, heartbeat_latency_ms, MAX(timestamp) 
                        FROM metrics GROUP BY node_id
                    ) m ON n.node_id = m.node_id
                """)
                for row in cursor.fetchall():
                    state_color = "green" if row["status"] == "IDLE" else "yellow" if row["status"] == "TRAINING" else "red"
                    
                    table.add_row(
                        row["node_id"],
                        row["ip_address"],
                        f"[{state_color}]{row['status']}[/]",
                        f"{row['cpu_percent'] or 0:.1f}%",
                        f"{row['ram_percent'] or 0:.1f}%",
                        f"{row['gpu_utilization'] or 0:.1f}%",
                        f"{row['heartbeat_latency_ms'] or 0:.1f}ms"
                    )
        except Exception:
            table.add_row("Error reading DB", "", "", "", "", "", "")
            
        return table

    def generate_jobs_table(self) -> Table:
        table = Table(title="Jobs", show_lines=True)
        table.add_column("Job ID", style="cyan")
        table.add_column("Nodes", justify="right")
        table.add_column("Status", style="bold")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT job_id, required_nodes, status FROM jobs ORDER BY created_at DESC LIMIT 5")
                for row in cursor.fetchall():
                    status_color = "yellow" if row["status"] == "PENDING" else "cyan" if row["status"] == "RUNNING" else "green" if row["status"] == "COMPLETED" else "red"
                    table.add_row(
                        row["job_id"],
                        str(row["required_nodes"]),
                        f"[{status_color}]{row['status']}[/]"
                    )
        except Exception:
            table.add_row("Error reading DB", "", "")
            
        return table

    def generate_logs_panel(self) -> Panel:
        log_lines = []
        try:
            if os.path.exists("logs/cluster.log"):
                with open("logs/cluster.log", "r") as f:
                    # Get last 10 lines
                    lines = f.readlines()[-10:]
                    for line in lines:
                        try:
                            data = json.loads(line)
                            log_lines.append(f"[{data.get('timestamp', '')}] [{data.get('node', '')}] {data.get('message', '')}")
                        except:
                            log_lines.append(line.strip())
        except Exception:
            pass
            
        content = "\n".join(log_lines) if log_lines else "No logs yet."
        return Panel(content, title="Recent Logs", style="white")

    def make_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="upper", ratio=1),
            Layout(name="lower", ratio=1)
        )
        layout["upper"].split_row(
            Layout(name="nodes", ratio=2),
            Layout(name="jobs", ratio=1)
        )
        
        layout["nodes"].update(self.generate_node_table())
        layout["jobs"].update(self.generate_jobs_table())
        layout["lower"].update(self.generate_logs_panel())
        
        return layout

if __name__ == "__main__":
    dashboard = ClusterDashboard()
    try:
        with Live(dashboard.make_layout(), refresh_per_second=1) as live:
            while True:
                time.sleep(1)
                live.update(dashboard.make_layout())
    except KeyboardInterrupt:
        pass
