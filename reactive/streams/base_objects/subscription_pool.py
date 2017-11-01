'''
A subscription pool is used to manage subscriptions and get work from a stream.
It manages multiple subscriptions.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.message.router_messages import Subscribe, DeSubscribe
from reactive.message.stream_messages import Cancel
from reactive.streams.base_objects.subscription import Subscription
from reactive.error.handler import handle_actor_system_fail


class SubscriptionPool(BaseActor):
    """
    Subscription pool containing subscriptions for an actor.
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.__subscriptions = []
    
    def subscribe(self, subscription):
        """
        Add a subscription to the pool.
        """
        if subscription not in self.__subscriptions:
            self.__subscriptions.append(subscription)
    
    def next(self):
        """
        Implemented by the user. Returns the next object in the result queue.

        :return: Returns the next object
        :rtype: object
        """
        return None

    def has_subscription(self, subscription):
        """
        Check whether a subscription is in the pool.
        """
        if subscription in self.__subscriptions:
            return True
        return False

    def remove_subscription(self, subscription):
        """
        Remove a subscription from the pool.
        """
        if self.has_subscription(subscription):
            self.__subscriptions.remove(subscription)

    def cancel(self, subscription):
        """
        Cancel and remove a subscription.

        :param subscription: The subscription to cancel and remove
        :type subscription: Subscription
        """
        if self.has_subscription(subscription):
            self.send(subscription, Cancel)
            self.remove_subscription(subscription)

    def receiveMessage(self, msg, sender):
        """
        Handle message on receipt.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :tpye sender: BaseActor
        """
        try:
            if isinstance(msg, Subscribe):
                sub = msg.payload
                if isinstance(sub, Subscription):
                    self.subscribe(sub)
            elif isinstance(msg, DeSubscribe):
                sub = msg.payload
                if isinstance(sub, Subscription):
                    self.remove_subscription(sub)
            elif isinstance(msg, Cancel):
                cncl = msg.payload
                if isinstance(cncl, Subscription):
                    self.cancel(sub)
        except Exception:
            handle_actor_system_fail()
