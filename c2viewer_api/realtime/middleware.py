import asyncio
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

class RealtimeSupplier:

    # def checker(self):
    #     tasks = [
    #         asyncio.ensure_future(self.data_change_detection()),
    #     ]
    #
    #     asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
    #     asyncio.get_event_loop().run_forever()

    async def data_change_detection(self):
        channel_layer = get_channel_layer()
        while True:
            await channel_layer.group_send(
                'tracker',
                {'type': 'send.message', 'message': 'hai'}
            )
            asyncio.sleep(3)