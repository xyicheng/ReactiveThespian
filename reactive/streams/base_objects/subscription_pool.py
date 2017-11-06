'''
A subscription pool is used to manage subscriptions and get work from a stream.
It manages multiple subscriptions.

Created on Nov 1, 2017

@author: aevans
'''

from queue import Queue
from time import sleep

from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import Subscribe, DeSubscribe
from reactive.message.stream_messages import Cancel, Pull, Push, GetDropPolicy,\
    GetSubscribers
import pdb


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
        self.__empty_batch_wait = 2
        self.__empty_times = 0
        self.__default_queue_size = 1000
        self.__result_q = Queue(maxsize=self.__default_queue_size)
        self.__drop_policy ="ignore"

    def get_default_queue_size(self):
        """
        Return the default queue size

        :return: Get the default queue size
        :type: int()
        """
        return self.__default_queue_size

    def get_result_q(self):
        """
        Obtain the result queue.
        :return: The result queue
        :rtype: list()
        """
        return self.__result_q

    def get_subscriptions(self):
        """
        Get the subscription
        :return: The current subscriptions
        :rtype: list()
        """
        return self.__subscriptions

    def set_drop_policy(self, msg, sender):
        """
        Set the drop policy

        :param msg: The message with the policy
        :type msg: Message
        :param sender: The message Sender
        :type sender: BaseActor
        """
        payload = msg.payload
        if isinstance(payload, str):
            if payload in ["pop", "ignore"]:
                self.__drop_policy = payload

    def get_drop_policy(self, msg, sender):
        """
        Get the drop policy

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        msg = GetDropPolicy(self.__drop_policy, sender, self)
        self.send(sender, msg)

    def subscribe(self, subscription):
        """
        Add a subscription to the pool.

        :param subscription: The subscription
        :type subscription: Subscription
        """
        if subscription not in self.__subscriptions:
            self.__subscriptions.append(subscription)

    def next(self, msg, sender):
        """
        Implemented by the user. Returns the next object in the result queue.

        :return: Returns the next object
        :rtype: object
        """
        pass

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

    def handle_push(self, msg, sender):
        """
        Handle a push request to the queue.
        """
        batch = msg.payload
        if isinstance(batch, list):
            if len(batch) > 0:
                self.__empty_times = 0
                for result in batch:
                    if self.__result_q.full():
                        if self.__drop_policy == "pop":
                            try:
                                self.__result_q.get_nowait()
                            except Exception:
                                pass
                    if self.__result_q.full() is False:
                        self.__result_q.put_nowait(result)
            else:
                self.__empty_times += 1
                if self.__empty_times >= len(self.__subscriptions): 
                    twait = self.__empty_batch_wait
                    sleep(twait)
                    self.__empty_batch_wait = 0
                else:
                    if len(self.__subscriptions) > 0:
                        twait = self.__empty_batch_wait / len(self.__subscriptions)
                        sleep(twait)

    def get_subscribers(self, msg, sender):
        """
        Get the list of current subscribers

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        msg = GetSubscribers(self.__subscriptions, sender, self)
        self.send(sender, msg)

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
                self.subscribe(sub)
            elif isinstance(msg, DeSubscribe):
                sub = msg.payload
                self.remove_subscription(sub)
            elif isinstance(msg, Pull):
                self.next(msg, sender)
            elif isinstance(msg, Push):
                self.handle_push(msg, sender)
            elif isinstance(msg, Cancel):
                self.cancel(msg)
            elif isinstance(msg, GetDropPolicy):
                self.get_drop_policy(msg, sender)
            elif isinstance(msg, GetSubscribers):
                self.get_subscribers(msg, sender)
        except Exception:
            handle_actor_system_fail()
