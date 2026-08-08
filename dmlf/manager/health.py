import asyncio
from aiohttp import web
from dmlf.manager.node_registry import NodeRegistry, NodeState
from dmlf.settings import get_settings


async def health_handler(request):
    return web.json_response({"status": "ok"})


async def ready_handler(request):
    registry: NodeRegistry = request.app["registry"]
    # Consider ready if DB reachable and at least one node IDLE
    try:
        nodes = registry.get_available_nodes()
        if nodes:
            return web.json_response({"status": "ready", "idle_nodes": len(nodes)})
        else:
            return web.json_response({"status": "not_ready", "idle_nodes": 0}, status=503)
    except Exception as e:
        return web.json_response({"status": "error", "detail": str(e)}, status=500)


def create_health_app(registry: NodeRegistry) -> web.Application:
    app = web.Application()
    app["registry"] = registry
    app.router.add_get("/health", health_handler)
    app.router.add_get("/ready", ready_handler)
    return app


async def run_health_server(app: web.Application, port: int):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    # Keep running
    await asyncio.Event().wait()