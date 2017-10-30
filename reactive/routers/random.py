'''
Random Router. This router randomly chooses an actor to push to.

Created on Oct 29, 2017

@author: aevans
'''

from reactive.routers.pub_sub import PubSub
from reactive.error.handler import handle_actor_system_fail

class RandoRouter(PubSub):

    def __init__(self):
        super().__init__()

    def receiveMessage(self, msg, sender):
        try:
            pass
        except:
            handle_actor_system_fail()
