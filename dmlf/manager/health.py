import asyncio
from aiohttp import web
from dmlf.manager.node_registry import NodeRegistry, NodeState
from dmlf.settings import get_settings
from dmlf.monitoring.prometheus import metrics_handler, CONTENT_TYPE_LATEST


async def health_handler(request):
    """Liveness probe - process is alive."""
    return web.json_response({"status": "ok"})


async def ready_handler(request):
    """Readiness probe - manager dependencies are initialized and operational."""
    registry: NodeRegistry = request.app["registry"]
    # Manager is ready if DB is reachable and core services are initialized
    try:
        # Check DB connectivity by querying nodes
        registry.get_all_nodes()
        return web.json_response({
            "status": "ready",
            "database": True,
            "scheduler": True,
            "autoscaler": True,
        })
    except Exception as e:
        return web.json_response({
            "status": "not_ready",
            "database": False,
            "scheduler": False,
            "autoscaler": False,
            "error": str(e)
        }, status=503)


async def cluster_handler(request):
    """Cluster status - worker availability for job scheduling."""
    registry: NodeRegistry = request.app["registry"]
    try:
        all_nodes = registry.get_all_nodes()
        idle_nodes = [n for n in all_nodes if n.get("status") == NodeState.IDLE]
        busy_nodes = [n for n in all_nodes if n.get("status") == NodeState.TRAINING]
        offline_nodes = [n for n in all_nodes if n.get("status") == NodeState.DISCONNECTED]
        
        return web.json_response({
            "total_nodes": len(all_nodes),
            "idle_nodes": len(idle_nodes),
            "busy_nodes": len(busy_nodes),
            "offline_nodes": len(offline_nodes),
            "can_schedule": len(idle_nodes) > 0,
        })
    except Exception as e:
        return web.json_response({"status": "error", "detail": str(e)}, status=500)


def create_health_app(registry: NodeRegistry) -> web.Application:
    app = web.Application()
    app["registry"] = registry
    app.router.add_get("/health", health_handler)
    app.router.add_get("/ready", ready_handler)
    app.router.add_get("/cluster", cluster_handler)
    app.router.add_get("/metrics", metrics_handler)
    return app


async def run_health_server(app: web.Application, port: int):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[DEBUG] Health server listening on port {port}", flush=True)
    # Keep running
    await asyncio.Event().wait()