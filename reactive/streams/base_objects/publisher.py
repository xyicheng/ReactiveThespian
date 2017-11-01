'''
A reactive streams oriented Publisher. This is a special router which contains
any routing logic (BROADCAST, ROUND_ROBING, RANDOM; etc.). The router contains
subscriptions instead of routees. Publish requests are sent to subscribers.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import Subscribe
from reactive.routers.router_type import RouterType


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
        self.__subscription = []

    def subscribe(self, subscription):
        """
        Subscribe a routee

        :param routee: The routee to use
        :type routee: BaseActor
        """
        try:
            if subscription not in self.__subscriptions:
                self.__subscriptions.append(subscription)
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
                if isinstance(actor, BaseActor):
                    self.subscribe(actor)
        except Exception:
            handle_actor_system_fail()
