from channels.generic.websocket import AsyncWebsocketConsumer
import json
import asyncio


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


class RealtimeTracker(AsyncWebsocketConsumer):
    channels = []

    async def connect(self):
        # add channel to list
        self.channels.append(self.channel_name)

        # Join room group
        await self.channel_layer.group_add(
            'tracker',
            self.channel_name
        )
        print("JOINED CHANNEL", self.channels)

        await self.accept()



    async def disconnect(self, close_code):
        # add channel to list
        self.channels.remove(self.channel_name)

        # Leave room group
        # pass
        await self.channel_layer.group_discard(
            'tracker',
            self.channel_name
        )

        print("REMAINING CHANNEL", self.channels)


    async def receive(self, text_data):
        message = json.loads(text_data)
        print("MESSAGE", message)
        print("FROM", self.channel_name)
