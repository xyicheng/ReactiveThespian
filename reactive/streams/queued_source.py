'''
Created on Nov 1, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail


class QueuedSource(BaseActor):

    def __init__(self):
        super().__init__()

    def receiveMessage(self, msg, sender):
        super().receiveMessage(msg, sender)
        try:
            pass
        except Exception:
            handle_actor_system_fail()
