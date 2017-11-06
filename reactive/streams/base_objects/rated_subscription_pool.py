'''
This subscription pool obtains record over time.

Created on Nov 5, 2017

@author: aevans
'''

from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import SetDropPolicy, Pull, Push
from reactive.streams.base_objects.subscription_pool import SubscriptionPool
from reactive.message.router_messages import Subscribe, DeSubscribe
from asyncio.events import Handle

class SubscriptionRate():
    """
    The subscription rate
    """
    def __init__(self, subscription, rate):
        """
        Constructor

        :param subscription: The subscription
        :type subscription: Subscription
        :param rate: The current rate
        :type rate: int()
        """
        self.subscription = None
        self.rate = [rate]


class RatedSubscriptionPool(SubscriptionPool):
    """
    Rated subscription pool
    """
    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.__avail = []
        self.__waiting = []

    def subscribe(self, subscription):
        """
        Add a subscription

        :param subscription: The subscription
        :type subscription: Subscription
        """
        sbr = SubscriptionRate(subscription, 1)
        self.__waiting.append(sbr) 

    def remove_subscription(self, subscription):
        """
        Remove a subscription

        :param subscription: The subscription
        :type subscription: Subscription
        """
        found = []
        for sub in self.__avail:
            if sub.subscription == subscription:
                found.append(sub)
        if len(found) > 0:
            for sub in found:
                self.__avail.remove(sub)

        found = []
        for sub in self.__waiting:
            if sub == subscription:
                found.append(sub)
        if len(found) > 0:
            for sub in found:
                self.__waiting.remove(sub)

    def __remake_available(self):
        """
        Recreate the available subscription list
        """
        self.__avail = sorted(self.__waiting, key=lambda x: int(sum(x.rate)/3))
        self.__waiting = []

    def next(self, msg):
        """
        Get the next batch

        :param msg: The message
        :type msg: Message
        """
        if msg.sender:
            sender = msg.sender
        batch_size = msg.payload
        batch = []
        rq = super().__result_q
        pull_size = 0
        if rq.empty() is False:
            while rq.empty() is False and pull_size < batch_size:
                try:
                    val = rq.get_nowait()
                    batch.append(val)
                    pull_size += 1
                except Exception:
                    handle_actor_system_fail()
        subs = super().get_subscriptions()
        if pull_size > 0:
            if len(self.__avail) == 0:
                self.__remake_available()
            sub = self.__avail.pop(0)
            msg = Pull(pull_size, sub, self.myAddress)
            self.send(sub, msg)
        elif rq.empty() and len(subs) > 0:
            pull_size = int(500 / len(subs))
            for sub in subs:
                msg = Pull(pull_size, sub, self.myAddress)
                self.send(sender, msg)

    def handle_push(self, msg, sender):
        """
        Handle a pusho on the result queue.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        batch = msg.payload
        if isinstance(batch, list):
            for res in batch:
                if self.__result_q.full():
                    if self.__drop_policy == "pop":
                        try:
                            self.__result_q.get_nowait()
                        except:
                            handle_actor_system_fail()
                    if self.__result_q.full() is False:
                        self.__result_q.put_nowait(res)
