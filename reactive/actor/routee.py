'''
An actor routee. There are special ways of asking the routee.
Any actor intended to be inside a router should be a routee

Created on Oct 29, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.message.router_messages import RouteAsk, RouteTell, Broadcast,\
    BalancingAsk, BalancingTell
from reactive.error.handler import handle_actor_system_fail


class ActorRoutee(BaseActor):

    def __init__(self):
        super().__init__()

    def on_receive(self, msg, sender):
        """
        Handle a received message.

        :param msg: Handle the incoming message
        :type msg: Message
        :param sender: Sender
        :type sender: Actor
        """
        pass

    def receiveMessage(self, msg, sender):
        """
        Receive a message.

        :param msg: The Message
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor()
        """
        try:
            if isinstance(msg, RouteAsk):
                # block on receive and send return value back
                msg = msg.payload
                if msg.sender:
                    val = self.on_receive(msg, sender)
                    self.send(sender, val)
            elif isinstance(msg, RouteTell):
                self.on_receive(msg, msg.sender)
            elif isinstance(msg, BalancingTell):
                router = msg.router
                payload = msg.payload
                msg = payload.msg
                sender = payload.sender
                self.set_on_receive(msg, sender)
                self.send(router, RouteTell(self.myAddress, router, self.myAddress))
            elif isinstance(msg, BalancingAsk):
                router = msg.router
                payload = msg.payload
                msg = payload.msg
                sender = payload.sender
                val  = self.set_on_receive(msg, sender)
                self.send(sender, val)
                self.send(router, RouteTell(self.myAddress, router, self.myAddress))
            elif isinstance(msg, RouteTell) or isinstance(msg, Broadcast):
                msg = msg.payload
                self.on_receive(msg, sender)
        except:
            handle_actor_system_fail()
