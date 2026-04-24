import asyncio
from aiohttp import web
import random

async def handler(request):
    r = random.random()

    if r < 0.5:
        return web.json_response({"ok": True}, status=200)
    elif r < 0.9:
        return web.Response(text="error", status=503)
    else:
        await asyncio.sleep(2)
        return web.json_response({"slow": True}, status=200)


app = web.Application()
app.router.add_get("/", handler)

web.run_app(app, port=8080)