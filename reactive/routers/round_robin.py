'''
Round robin router that handles ask and tell messages in round robin fashion.
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

from reactive.message.router_messages import RouteTell, RouteAsk, Broadcast,\
    Subscribe, DeSubscribe
from reactive.routers.pub_sub import PubSub
from reactive.message.base_message import Message
from reactive.error.handler import handle_actor_system_fail,\
    format_message_error


class RoundRobinRouter(PubSub):
    """
    Round Robin Router is a special type of PubSub to push messages across a
    network.
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()

    def handle_message(self, msg, sender):
        """
        Handle an incoming message

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        if isinstance(msg, RouteTell):
            payload = msg.payload
            if payload and isinstance(payload, Message):
                if len(self.__actor_set) > 0:
                    actor = self.__actor_set[self.__index]
                    self.__index += 1
                    if self.__index == len(self.__actor_set):
                        self.__index = 0
                    msg = RouteTell(payload, actor, msg.sender)
                    self.send(actor, msg)

    def receiveMessage(self, msg, sender):
        """
        Handle the incoming messages

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
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
