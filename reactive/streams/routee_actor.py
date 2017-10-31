'''
The routee actor is used to handle work batches.

Created on Oct 31, 2017

@author: aevans
'''

import asyncio
from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Push


class RouteeActor(BaseActor):

    def __init__(self):
        self.loop = asyncio.new_event_loop()

    async def on_push(self):
        pass

    def receiveMessage(self, msg, sender):
        try:
            if isinstance(msg, Push):
                batch = msg.payload
        except Exception:
            handle_actor_system_fail()
