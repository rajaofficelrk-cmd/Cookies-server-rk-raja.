import asyncio
import json
import os
import time
import uuid

from aiohttp import web, WSMsgType


class DemoServer:
    def __init__(self):
        self.start_time = time.time()
        self.tasks = {}
        self.clients = set()

    # ---------- HTTP ----------

    async def index(self, request):
        return web.FileResponse("index.html")

    # ---------- WebSocket ----------

    async def websocket(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        self.clients.add(ws)

        try:
            await self.send(ws, {
                "type": "log",
                "message": "Connected to safe demo server"
            })

            async for message in ws:
                await self.handle_message(ws, message)

        finally:
            self.clients.discard(ws)

        return ws

    async def handle_message(self, ws, message):
        if message.type != WSMsgType.TEXT:
            return

        try:
            data = json.loads(message.data)
        except json.JSONDecodeError:
            return

        message_type = data.get("type")

        if message_type == "ping":
            await self.send(ws, {"type": "pong"})

        elif message_type == "monitor":
            await self.send_monitor(ws)

        elif message_type == "start":
            await self.start_task(ws)

        elif message_type == "stop_by_id":
            await self.stop_task(
                ws,
                data.get("taskId")
            )

    # ---------- Tasks ----------

    async def start_task(self, ws):
        task_id = str(uuid.uuid4())[:8]

        self.tasks[task_id] = {
            "started": time.time(),
            "sent": 0,
            "running": True,
        }

        await self.send(ws, {
            "type": "task_started",
            "taskId": task_id,
        })

        await self.send(ws, {
            "type": "log",
            "message": f"Demo task {task_id} started",
        })

        asyncio.create_task(
            self.run_demo_task(task_id)
        )

    async def run_demo_task(self, task_id):
        while self.is_task_running(task_id):
            await asyncio.sleep(2)

            if not self.is_task_running(task_id):
                break

            self.tasks[task_id]["sent"] += 1

            count = self.tasks[task_id]["sent"]

            await self.broadcast({
                "type": "log",
                "message": (
                    f"Demo message #{count} "
                    f"processed for task {task_id}"
                ),
            })

            await self.broadcast_monitor()

        self.tasks.pop(task_id, None)
        await self.broadcast_monitor()

    async def stop_task(self, ws, task_id):
        if not task_id:
            await self.send(ws, {
                "type": "log",
                "message": "Task ID required",
            })
            return

        task = self.tasks.get(task_id)

        if task is None:
            await self.send(ws, {
                "type": "log",
                "message": "Task ID not found",
            })
            return

        task["running"] = False

        await self.broadcast({
            "type": "stopped",
            "taskId": task_id,
        })

        await self.broadcast({
            "type": "log",
            "message": f"Demo task {task_id} stopped",
        })

    def is_task_running(self, task_id):
        task = self.tasks.get(task_id)
        return bool(task and task["running"])

    # ---------- Monitoring ----------

    def monitor_data(self):
        uptime = int(time.time() - self.start_time)

        total_sent = sum(
            task["sent"]
            for task in self.tasks.values()
        )

        return {
            "type": "monitor_data",
            "uptime": uptime,
            "activeTasks": len(self.tasks),
            "totalSent": total_sent,
        }

    async def send_monitor(self, ws):
        await self.send(
            ws,
            self.monitor_data()
        )

    async def broadcast_monitor(self):
        await self.broadcast(
            self.monitor_data()
        )

    # ---------- WebSocket Helpers ----------

    async def send(self, ws, data):
        try:
            await ws.send_json(data)
        except Exception:
            self.clients.discard(ws)

    async def broadcast(self, data):
        disconnected = set()

        for ws in self.clients:
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.add(ws)

        self.clients.difference_update(disconnected)


def create_app():
    server = DemoServer()

    app = web.Application()

    app.router.add_get(
        "/",
        server.index
    )

    app.router.add_get(
        "/ws",
        server.websocket
    )

    return app


app = create_app()


if __name__ == "__main__":
    port = int(
        os.environ.get("PORT", "8080")
    )

    web.run_app(
        app,
        host="0.0.0.0",
        port=port
    )
