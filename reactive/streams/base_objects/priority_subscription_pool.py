'''
Subscriptions in this pool are called on a priority basis, enabling some jobs
to take priority.

Created on Nov 5, 2017

@author: aevans
'''

from reactive.streams.base_objects.subscription_pool import SubscriptionPool
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull, Push, Cancel,\
    SubscribeWithPriority
from reactive.message.router_messages import Subscribe, DeSubscribe
from reactive.streams.base_objects.subscription import Subscription


class SubscriptionPriority:

    def __init__(self, subscription, priority):
        """
        Constructor

        :param subscription: The subsription
        :type subscription: Subscription
        :param priority: The priority
        :type priority: int() 
        """
        self.subscription = subscription
        self.default_priority = priority
        self.priority = priority


class PrioritySubscriptionPool(SubscriptionPool):

    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.__priority_queue = []
        self.__waiting_queue = []


    def remake_priority_make(self):
        """
        Re-create the priority queue
        """
        self.__priority_queue = list(
            sorted(self.__waiting_queue, key=lambda x: x.priority))
        self.__waiting_queue = []

    def next(self, msg, sender):
        """
        Get the next n elements in the batch.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        batch_size = msg.payload
        if msg.sender:
            sender = msg.sender
        batch = []
        rq = super().__result_q
        pull_size = 0
        if rq.empty() is False:
            while rq.empty() is False and pull_size < batch_size:
                try:
                    pull_size += 1
                    val = rq.get_nowait()
                    batch.append(val)
                except Exception:
                    handle_actor_system_fail()
        msg = Push(batch, sender, self.myAddress)
        self.send(sender, msg)
        subs = super().get_subscriptions()
        if pull_size > 0:
            if len(self.__priority_queue) == 0:
                self.remake_priority_queue()
            sub = self.__priority_queue.pop(0)
            self.__waiting_queue.append(sub)
            msg = Pull(pull_size, self)
            self.send(sub, msg)
        elif rq.empty() and len(subs) > 0:
            pull_size = int(500 / len(subs))
            for sub in subs:
                msg = Pull(pull_size, sub, self.myAddress)
                self.send(sender, msg)

    def handle_push(self, msg, sender):
        """
        Handle a push

        :param msg: The message
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        payload = msg.payload
        if isinstance(payload, list):
            rq = self.__result_q
            for result in payload:
                if rq.full():
                    if self.__drop_policy == "pop":
                        try:
                            rq.get_nowait()
                        except:
                            pass
                if rq.full() is False:
                    rq.put_nowait(result)

    def subscribe(self, msg, sender):
        """
        Subscribe. If the subscription exists,
        reset the default priority.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        subscription = msg.payload
        found = False
        i = 0
        sp = None
        while not found and i < len(self.__subscriptions):
            i += 1
            psp = self.__subscriptions[i]
            if sp.subscription == subscription:
                found = True
                sp = psp
        if sp:
            sp.priority = msg.default_priority
        else:
            sp = SubscriptionPriority(subscription, 0)
            self.__subscriptions.append(sp)
            self.__waiting_queue.append(sp)

    def desubscribe(self, msg, sender):
        """
        DeSubscribe

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        subscription = msg.payload
        i = 0
        while i < len(self.__subscriptions):
            sp = self.__subscriptions
            if subscription == sp.subscription:
                i = len(self.__subscriptions)
                self.__subscriptions.remove(sp)

    def receiveMessage(self, msg, sender):
        """
        Handle message on receipt.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :tpye sender: BaseActor
        """
        try:
            if isinstance(msg, SubscribeWithPriority):
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
                cncl = msg.payload
                self.cancel(sub)
        except Exception:
            handle_actor_system_fail()
