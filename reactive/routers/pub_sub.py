'''
Publisher Subscriber handles subscription and broadcast. It is meant
to be implemented across the system.

Created on Oct 29, 2017

@author: aevans
'''

from atomos.atomic import AtomicInteger
from reactive.actor.base_actor import BaseActor
from reactive.actor.routee import ActorRoutee
from reactive.message.router_messages import Subscribe, DeSubscribe, Broadcast
from reactive.error.handler import handle_actor_system_fail,\
    format_send_receive_error, format_message_error
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
                if payload and isinstance(payload, ActorRoutee):
                    self.on_subscribe(payload)
                elif payload and isinstance(payload, BaseActor):
                    routee = self.createActor(ActorRoutee)
                    routee.on_receive = payload.receiveMessage
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
                if payload and isinstance(payload, ActorRoutee):
                    self.de_subscribe(payload)
                else:
                    err_msg = "DeSubscribe Requires Base ActorRoutee Payload"
                    err_msg = format_send_receive_error(err_msg, sender, self)
                    raise ValueError(err_msg)
        except:
            handle_actor_system_fail()

    def handle_broadcast(self, msg, sender):
        """
        Handle a message.

        :param msg: Message to support
        :type msg: Message()
        :param sender: The sender to build
        :type sender: Actor()
        """
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

    def handle_unexpected_message(self, msg, sender):
        """
        Handle any unexpected messages.

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The sender
        :type sender: BaseActor()
        """
        err_msg = "Unexpected Message in {}.\nType={}\nSender={}"
        err_msg = err_msg.format(str(self), str(type(msg)), str(sender))

    def check_message_and_sender(self, msg, sender):
        """
        Ensure that the message and sender exist.
        """
        if msg is None or not isinstance(msg, Message):
                err_msg = format_message_error(Message, str(type(msg)))
                err_msg = "Messages Differ From Expected in {}\n{}".format(
                    str(self), err_msg)
                raise ValueError(err_msg)

        if msg.sender is None:
            msg.sender = sender

    def receiveMessage(self, msg, sender):
        """
        Handle the incoming messages
        """
        try:
            self.check_message_and_sender(msg, sender)
            if isinstance(msg, Subscribe):
                self.handle_subscription(msg, sender)
            elif isinstance(msg, DeSubscribe):
                self.handle_desubscribe(msg, sender)
            elif isinstance(msg, Broadcast):
                self.handle_broadcast(msg, sender)
            else:
                self.handle_unexpected_message(msg, sender)
        except:
            handle_actor_system_fail()
