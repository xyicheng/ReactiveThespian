'''
Publisher Subscriber handles subscription and broadcast. It is meant
to be implemented across the system.

Created on Oct 29, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from atomos.atomic import AtomicInteger
from reactive.message.router_messages import Subscribe, DeSubscribe, Broadcast
from reactive.error.handler import handle_actor_system_fail,\
    format_send_receive_error
from reactive.message.base_message import Message


class PubSub(BaseActor):
    """
    PubSub for message passing.
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.__actor_set = []
        self.num_actors = AtomicInteger(0)
        self.__index = 0

    def on_subscribe(self, actor):
        """
        Subscribe to the pub/sub
        """
        if actor not in self.actor_set:
            self.actor_set.append(actor)
            self.num_actors.get_and_add(1)

    def de_subscribe(self, actor):
        """
        De-subscribe an actor
        """
        if actor in self.actor_set:
            self.actor_set.remove(actor)
            self.num_actors.get_and_add(1)
            if self.__index >= len(self.__actor_set):
                self.__index = 0

    def handle_subscription(self, msg, sender):
        """
        Handle a subscription message.

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The sender of the message
        :type sender: Actor()
        """
        try:
            if isinstance(msg, Subscribe):
                payload = msg.payload
                if payload and isinstance(payload, BaseActor):
                    self.on_subscribe(payload)
                else:
                    err_msg = "Subscribe Requires Base Actor Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
        except:
            handle_actor_system_fail()

    def handle_desubscribe(self, msg, sender):
        """
        Handle a desubscription message.

        :param msg: The Message to handle
        :type msg: Message()
        :param sender: Sender of a Desubscribe message
        :type sender: Actor()
        """
        try:
            if isinstance(msg, DeSubscribe):
                payload = msg.payload
                if payload and isinstance(payload, BaseActor):
                    self.de_subscribe(payload)
                else:
                    err_msg = "DeSubscribe Requires Base Actor Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
        except:
            handle_actor_system_fail()

    def handle_message(self, msg, sender):
        try:
            if isinstance(msg, Broadcast):
                payload = msg.payload()
                if payload and isinstance(payload, Message):
                    for actor in self.__actor_set:
                        self.sys.tell(actor, payload)
                else:
                    err_msg = "Broadcast Requires Message Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
        except:
            handle_actor_system_fail()

    def receiveMessage(self, msg, sender):
        """
        Handle the incoming messages
        """
        if isinstance(msg, Subscribe):
            self.handle_subscription(msg, sender)
        elif isinstance(msg, DeSubscribe):
            self.handle_desubscribe(msg, sender)
        else:
            self.handle_message(msg, sender)
