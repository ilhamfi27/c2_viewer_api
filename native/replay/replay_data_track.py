#!/usr/bin/env python

# WS server example that synchronizes REALTIME_state across clients
from main import *
from replay_generator_chunked import *

get_replay()

# USERS = set()
# replay_data_send = []

# async def ambil_replay():
#     get_replay()
#     if USERS:
#         message = json.dumps(replay_data_send, default=str)
#         await asyncio.wait([user.send(message) for user in USERS])

# async def send_reply_data():
#     if USERS:
#         message = json.dumps(replay_data_send, default=str)
#         await asyncio.wait([user.send(message) for user in USERS])

# async def register(websocket):
#     USERS.add(websocket)
#     print(USERS)

# async def unregister(websocket):
#     USERS.remove(websocket)
# async def handler(websocket, path):
#     await register(websocket),
#     try:
#         await send_reply_data()
#         async for message in websocket:
#             pass
#     except websockets.exceptions.ConnectionClosedError:
#         print("connection error")
#     finally:
#         await unregister(websocket)

# # start_server = websockets.serve(handler, "10.20.112.217", 8080)
# # start_server = websockets.serve(handler, "192.168.43.14", 14045)
# start_server = websockets.serve(handler, "127.0.0.1", 8082)

# tasks = [
#     asyncio.ensure_future(start_server),
#     asyncio.ensure_future(ambil_replay())
# ]

# asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
# asyncio.get_event_loop().run_forever()

