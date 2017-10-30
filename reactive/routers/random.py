'''
Random Router. This router randomly chooses an actor to push to.
Routers are somewhat inefficient in that RouteTell and RouteAsk
must be used to avoid as much blocking as possible. This will
improve throughput but add a small dose of complexity.

To ask, call ask to block and wait for a return and use RouteAsk.
To tell, call tell and use RouteTell. 

This will cause the ActorRoutee actors to performing the correct
behavior.

Created on Oct 29, 2017

@author: aevans
'''

from reactive.routers.pub_sub import PubSub
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import RouteTell, RouteAsk, Subscribe,\
    DeSubscribe, Broadcast
from random import Random

class RandomRouter(PubSub):

    def __init__(self):
        """
        Constructor
        """
        super().__init__()

    def handle_message(self, msg, sender):
        """
        Handle a tell request.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        actor = Random().choice(self.__actor_set)
        self.send(actor, msg)

    def receiveMessage(self, msg, sender):
        try:
            self.check_message_and_sender(msg, sender)
            if isinstance(msg, RouteTell) or isinstance(msg, RouteAsk):
                self.handle_message(msg, sender)
            elif isinstance(msg, Subscribe):
                self.handle_subscription(msg, sender)
            elif isinstance(msg, DeSubscribe):
                self.handle_desubscribe(msg, sender)
            elif isinstance(msg, Broadcast):
                self.handle_broadcast(msg, sender)
            else:
                err_msg = "PubSub Does not Understand {}\n{}"
                err_msg = err_msg.format(str(type(msg)), str(self))
                raise ValueError(err_msg)
        except:
            handle_actor_system_fail()
