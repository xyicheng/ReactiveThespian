'''
A reactive streams oriented Publisher. This is a special router which contains
any routing logic (BROADCAST, ROUND_ROBING, RANDOM; etc.). The router contains
subscriptions instead of routees. Publish requests are sent to subscribers.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import Subscribe, DeSubscribe
from reactive.streams.base_objects.subscription import Subscription
from reactive.message.stream_messages import SetPublisher, SetDropPolicy


class Publisher(BaseActor):
    """
    Publisher. Publishes messages to subscribers.
    """

    def __init__(self):
        """
        Constructor

        :param router: The router to use.  All extend PubSub which is default.
        :type router: PubSub
        """
        self.__subscriptions = []
        self.__publisher = None
        self.drop_policy = "ignore"

    def subscribe(self, subscription):
        """
        Subscribe a subscription actor

        :param subscription: The subscription to use
        :type subscription: Subscription
        """
        try:
            if isinstance(subscription, Subscription):
                sub = Subscribe(subscription, self.__pool, self.myAddress)
                self.send(self.__pool, sub)
        except Exception:
            handle_actor_system_fail()

    def desubscribe(self, subscription):
        """
        DeSubscribe a subscription actor

        :param subscription: The actor to use
        :type subscription: Subscription
        """
        try:
            if isinstance(subscription, Subscription):
                sub = DeSubscribe(subscription, self.__pool, self.myAddress)
                self.send(self.__pool, sub)
        except Exception:
            handle_actor_system_fail()

    def set_publisher(self, msg, sender):
        """
        Set the publisher

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        payload = msg.payload
        if isinstance(payload, BaseActor):
            self.__publisher = payload 

    def set_drop_policy(self, msg, sender):
        """
        Set the drop policy 
        """
        payload = msg.payload
        if isinstance(payload, str):
            self.drop_policy = payload

    def receiveMessage(self, msg, sender):
        """
        Handle message on receipt.

        :param msg: The message received
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        try:
            if isinstance(msg, Subscribe):
                actor = msg.payload
                self.subscribe(actor)
            elif isinstance(msg, DeSubscribe):
                actor = msg.payload
                self.desubscribe(actor)
            elif isinstance(msg, SetPublisher):
                self.set_publisher(msg, sender)
            elif isinstance(msg, SetDropPolicy):
                self.set_drop_policy(msg, sender)
        except Exception:
            handle_actor_system_fail()
