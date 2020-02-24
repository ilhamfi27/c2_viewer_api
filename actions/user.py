import asyncio
import json

async def send_message(USERS, message_data, data_category):
    if USERS:
        send_data = dict()
        send_data[data_category] = message_data
        message = json.dumps({'data': send_data, 'data_type': 'realtime'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])

async def send_notification(USERS, message_data, data_category):
    if USERS:
        send_data = dict()
        send_data[data_category] = message_data
        message = json.dumps({'data': send_data, 'data_type': 'notification'}, default=str)
        await asyncio.wait([user.send(message) for user in USERS])
