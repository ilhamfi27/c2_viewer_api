from channels.generic.websocket import AsyncWebsocketConsumer
import json


class RealtimeAccess(AsyncWebsocketConsumer):
    async def connect(self):
        # Join room group
        await self.channel_layer.group_add(
            'realtime',
            self.channel_name
        )
        print("JOINED")

        await self.accept()


    async def disconnect_client(self, message=None):
        print("SENDING MESSAGE", message['message'])
        await self.send(text_data=message['message'])


    async def disconnect(self, close_code):
        # Leave room group
        # pass
        await self.channel_layer.group_discard(
            'realtime',
            self.channel_name
        )
