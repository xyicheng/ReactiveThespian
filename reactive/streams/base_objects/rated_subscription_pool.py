'''
This subscription pool obtains record over time.

Created on Nov 5, 2017

@author: aevans
'''

from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull, Push
from reactive.streams.base_objects.subscription_pool import SubscriptionPool
from time import sleep
import pdb


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
        self.subscription = subscription
        self.rate = rate
        self.defualt_rate = rate
        self.__empty_batch_wait = .2
        self.__empty_times = 0


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
        self.__empty_times = 0
        self.__empty_batch_wait = .005

    def subscribe(self, subscription):
        """
        Add a subscription

        :param subscription: The subscription
        :type subscription: Subscription
        """
        sbr = SubscriptionRate(subscription, 1)
        self.__waiting.append(sbr)
        self.get_subscriptions().append(sbr)

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
                if sub in self.get_subscriptions():
                    self.get_subscriptions().remove(sub)
                self.__avail.remove(sub)

        found = []
        for sub in self.__waiting:
            if sub == subscription:
                if sub in self.get_subscriptions():
                    self.get_subscriptions().remove(sub)
                found.append(sub)
        if len(found) > 0:
            for sub in found:
                self.__waiting.remove(sub)

    def __remake_available(self):
        """
        Recreate the available subscription list
        """
        self.__avail = list(sorted(self.__waiting, key=lambda x: x.rate))
        self.__waiting = []

    def next(self, msg, sender):
        """
        Get the next batch

        :param msg: The message
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        batch_size = msg.payload
        batch = []
        rq = self.get_result_q()
        pull_size = 0
        if rq.empty() is False:
            while rq.empty() is False and pull_size < batch_size:
                try:
                    val = rq.get_nowait()
                    batch.append(val)
                    pull_size += 1
                except Exception:
                    handle_actor_system_fail()
        msg = Push(batch, sender, self)
        self.send(sender, msg)
        subs = self.get_subscriptions()
        if pull_size > 0:
            if len(self.__avail) == 0:
                self.__remake_available()
            sub = self.__avail.pop(0)
            self.__waiting.append(sub)
            subscription = sub.subscription
            msg = Pull(pull_size, subscription, self.myAddress)
            self.send(subscription, msg)
        elif rq.empty() and len(subs) > 0:
            pull_size = int(500 / len(subs))
            for sub in subs:
                subscription = sub.subscription
                msg = Pull(pull_size, subscription, self.myAddress)
                self.send(subscription, msg)

    def handle_push(self, msg, sender):
        """
        Handle a push on the result queue.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        batch = msg.payload
        if isinstance(batch, list):
            if len(batch) > 0:
                for res in batch:
                    if self.get_result_q().full():
                        if self.__drop_policy == "pop":
                            try:
                                self.get_result_q().get_nowait()
                            except:
                                handle_actor_system_fail()
                    if self.get_result_q().full() is False:
                        self.get_result_q().put_nowait(res)
                sp = None
                for sub in self.get_subscriptions():
                    if sub.subscription == sender:
                        sp = sub
                if sp:
                    if len(batch) is 0:
                        sp.rate = max([sp.rate - 1, 0])
                    else:
                        sp.rate = min([sp.rate + 1, sp.defualt_rate])
            else:
                self.__empty_times += 1
                if self.__empty_times >= len(self.get_subscriptions()):
                    self.__empty_times = 0
                    sleep(self.__empty_batch_wait)
