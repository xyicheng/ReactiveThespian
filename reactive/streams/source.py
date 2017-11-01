'''
Created on Oct 29, 2017

@author: aevans
'''

from queue import Queue
from reactive.actor.base_actor import BaseActor
from reactive.message.stream_messages import Pull, Push
import pdb


class Source(BaseActor):

    def __init__(self):
        pass

    def on_pull(self, payload):
        pass

    def receiveMessage(self, msg, sender):

        if isinstance(msg, Pull):
            batch = []
            batch_size = msg.payload
            i = 0
            val = True
            while i < batch_size and val:
                val = self.on_pull(msg.payload)
                if val:
                    batch.append(val)
                i += 1
            self.send(sender, Push(batch, sender, self))
