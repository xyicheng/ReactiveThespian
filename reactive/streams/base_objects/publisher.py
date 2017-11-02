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
from reactive.routers.router_type import RouterType
from reactive.streams.base_objects.round_robin_subscription_pool import RoundRobinSubscriptionPool
from reactive.streams.base_objects.subscription import Subscription


class Publisher(BaseActor):
    """
    Publisher. Publishes messages to subscribers.
    """

    def __init__(self, routing_logic=RouterType.BROADCAST, subscription_pool=RoundRobinSubscriptionPool()):
        """
        Constructor

        :param router: The router to use.  All extend PubSub which is default.
        :type router: PubSub
        """
        self.__routing_logic = routing_logic
        self.__pool = subscription_pool

    def subscribe(self, subscription):
        """
        Subscribe a subscription actor

        :param subscription: The subscription to use
        :type subscription: Subscription
        """
        try:
            if isinstance(subscription, Subscription):
                sub = Subscribe(subscription, self.__pool, self)
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
                sub = DeSubscribe(subscription, self.__pool, self)
                self.send(self.__pool, sub)
        except Exception:
            handle_actor_system_fail()

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
        except Exception:
            handle_actor_system_fail()
