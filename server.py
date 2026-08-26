import asyncio
import json
import time
import uuid

from aiohttp import web, WSMsgType

START_TIME = time.time()
TASKS = {}
CLIENTS = set()


async def index(request):
    return web.FileResponse("index.html")


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    CLIENTS.add(ws)

    try:
        await ws.send_json({
            "type": "log",
            "message": "Connected to safe demo server"
        })

        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue

            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "ping":
                await ws.send_json({"type": "pong"})

            elif msg_type == "monitor":
                await send_monitor(ws)

            elif msg_type == "start":
                task_id = str(uuid.uuid4())[:8]

                TASKS[task_id] = {
                    "started": time.time(),
                    "sent": 0,
                    "running": True
                }

                await ws.send_json({
                    "type": "task_started",
                    "taskId": task_id
                })

                await ws.send_json({
                    "type": "log",
                    "message": f"Demo task {task_id} started"
                })

                asyncio.create_task(
                    demo_task(task_id)
                )

            elif msg_type == "stop_by_id":
                task_id = data.get("taskId")

                if task_id in TASKS:
                    TASKS[task_id]["running"] = False

                    await broadcast({
                        "type": "stopped",
                        "taskId": task_id
                    })

                    await broadcast({
                        "type": "log",
                        "message": f"Demo task {task_id} stopped"
                    })
                else:
                    await ws.send_json({
                        "type": "log",
                        "message": "Task ID not found"
                    })

    finally:
        CLIENTS.discard(ws)

    return ws


async def demo_task(task_id):
    while task_id in TASKS and TASKS[task_id]["running"]:
        await asyncio.sleep(2)

        if task_id not in TASKS:
            break

        if not TASKS[task_id]["running"]:
            break

        TASKS[task_id]["sent"] += 1

        await broadcast({
            "type": "log",
            "message": (
                f"Demo message #{TASKS[task_id]['sent']} "
                f"processed for task {task_id}"
            )
        })

        await broadcast_monitor()

    TASKS.pop(task_id, None)


async def send_monitor(ws):
    uptime = int(time.time() - START_TIME)

    total_sent = sum(
        task["sent"] for task in TASKS.values()
    )

    await ws.send_json({
        "type": "monitor_data",
        "uptime": uptime,
        "activeTasks": len(TASKS),
        "totalSent": total_sent
    })


async def broadcast_monitor():
    if not CLIENTS:
        return

    uptime = int(time.time() - START_TIME)

    total_sent = sum(
        task["sent"] for task in TASKS.values()
    )
