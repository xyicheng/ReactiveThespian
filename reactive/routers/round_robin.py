'''
Round robin router that handles ask and tell messages in round robin fashion.

Created on Oct 29, 2017

@author: aevans
'''

from reactive.message.router_messages import RouteTell, RouteAsk, Broadcast,\
    Subscribe, DeSubscribe
from reactive.routers.PubSub import PubSub
from reactive.message.base_message import Message
from reactive.actor.base_actor import BaseActor
from reactive.error.handler import format_send_receive_error


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

    def handle_tell(self, msg, sender):
        """
        Handle an incoming tell message

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        pass

    def handle_ask(self, msg, sender):
        """
        Handle an incoming ask message.

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        pass

    def receiveMessage(self, msg, sender):
        """
        Handle the incoming messages

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        try:
            if isinstance(msg, RouteTell):
                payload = msg.payload
                if payload and isinstance(payload, Message):
                    if len(self.__actor_set) > 0:
                        actor = self.__actor_set[self.__index]
                        self.__index += 1
                        if self.__index == len(self.__actor_set):
                            self.__index = 0
                        msg = RouteTell(payload, actor, msg.sender)
                        self.sys.tell(actor, msg)
            elif isinstance(msg, RouteAsk):
                payload = msg.payload
                if payload and isinstance(payload, Message):
                    if len(self.__actor_set) > 0:
                        actor = self.__actor_set[self.__index]
                        self.__index += 1
                        if self.__index == len(self.__actor_set):
                            self.__index = 0
                        msg = RouteAsk(payload, actor, msg.sender, msg.timeout)
                        self.sys.tell(actor, msg)
            elif isinstance(msg, Subscribe):
                payload = msg.payload
                if payload and isinstance(payload, BaseActor):
                    self.on_subscribe(payload)
                else:
                    err_msg = "Subscribe Requires Base Actor Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
            elif isinstance(msg, Broadcast):
                payload = msg.payload()
                if payload and isinstance(payload, Message):
                    for actor in self.__actor_set:
                        self.sys.tell(actor, payload)
                else:
                    err_msg = "Broadcast Requires Message Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)    
            elif isinstance(msg, DeSubscribe):
                payload = msg.payload
                if payload and isinstance(payload, BaseActor):
                    self.de_subscribe(payload)
                else:
                    err_msg = "DeSubscribe Requires Base Actor Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
            else:
                err_msg = "PubSub Does not Understand {}\n{}"
                err_msg = err_msg.format(str(type(msg)), str(self))
                raise ValueError(err_msg)
        except:
            handle_actor_system_fail()