'''
Created on Oct 29, 2017

@author: aevans
'''

from reactive.message.stream_messages import Push
from reactive.routers.pub_sub import PubSub
from reactive.message.router_messages import Subscribe, DeSubscribe
from thespian.actors import Actor


class Sink(PubSub):

    def __init__(self):
        pass

    def on_push(self, msg, sender):
        """
        Handle a push request.
        """
        pass

    def receiveMessage(self, msg, sender):
        if isinstance(msg, Push):
            self.on_push(msg, sender)
        elif isinstance(msg, Subscribe):
            actor = msg.payload
            if isinstance(actor, Actor):
                self.on_subscribe(actor)
        elif isinstance(msg, DeSubscribe):
            actor = msg.payload
            if isinstance(actor, Actor):
                self.de_subscribe(actor)
