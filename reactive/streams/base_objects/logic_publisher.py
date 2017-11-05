'''
A Logic processor stores a queue per condition. A default base queue is
supported as well.

Created on Nov 2, 2017

@author: aevans
'''

import marshal
from reactive.streams.base_objects.publisher import Publisher
from queue import Queue
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Push, SubscribeWithLogic,\
    PullBySubscription, SetPublisher, Pull
from reactive.message.router_messages import DeSubscribe
from reactive.streams.base_objects.subscription import Subscription


class LogicPublisher(Publisher):
    """
    Publisher that connects a subscriptions to their outputs. 
    """

    def __init__(self):
        """
        Constructor
        """
        self.__publisher
        self.__init__()
        self.__total_size = 0
        self.__subscriptions = {}
        self.__queues = {
            'default' : Queue()
        }

    def get_subscriber(self, val):
        """
        Go through subscribers until finding a subscriber where
        the resulting logic is True

        :param val: The value to check against
        :type val: The value
        :return: The value to check for a subscription with or None
        :rtype: Subscription
        """
        if self.__subscriptions:
            for k,v in self.__subscriptions.items():
                if v(val):
                    return k
        return None


    def on_push(self, msg, sender):
        """
        Handle a push message

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        payload = msg.sender
        if isinstance(payload, list):
            if len(payload) > 0:
                for result in payload:
                    sub = self.get_subscriber(result)
                    if sub:
                        if sub in self.__queues.keys():
                            rq = self.__queues[sub]
                            if rq.full():
                                if self.drop_policy == "pop":
                                    try:
                                        rq.get_nowait()
                                    except:
                                        pass
                            if rq.full() is False:
                                rq.put_nowait(result)

    def on_pull(self, msg, sender):
        """
        Handle a pull request

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        sender = msg.sender
        subscription = msg.subscription
        batch_size = msg.payload
        batch = []
        pull_size = 0
        if isinstance(subscription, Subscription):
            if subscription in self.__queues.keys():
                rq = self.__queues[subscription]
                if rq.empty() is False:
                    pull_size = 0
                    while rq.empty() is False and pull_size < batch_size:
                        val = rq.get_nowait()
                        batch.append(val)
                        pull_size += 1
            msg = Push(batch, sender, self)
            self.send(sender, msg)
            if pull_size > 0:
                msg = Pull(pull_size, self.__publisher, self)
                self.send(self.__publisher, msg)

    def subscribe(self, msg, sender):
        """
        Subscribe to the queues

        :param subscription: The subscription request
        :type subscription: Subscription
        """
        subscription = msg.payload
        logic = msg.logic
        marshal.loads(logic)
        if isinstance(subscription, Subscription):
            if subscription not in self.__subscriptions.keys():
                self.__subscriptions[subscription] = logic
                self.__queues[subscription] = Queue()

    def desubscribe(self, subscription):
        """
        DeSubscribe from the queues

        :param subscription: The subscription
        :type susbscription: Subscription
        """
        if isinstance(subscription, Subscription):
            if subscription in self.__subscriptions.keys():
                self.__subscriptions.pop(subscription)
            if subscription in self.__queues.keys():
                self.__queues.pop(subscription)

    def receiveMessage(self, msg, sender):
        """
        Handle a message.

        :param msg: Handle a receive message
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        try:
            if isinstance(msg, PullBySubscription):
                self.on_pull(msg, sender)
            elif isinstance(msg, Push):
                self.on_push(msg, sender)
            elif isinstance(msg, SetPublisher):
                self.set_publisher(msg, sender)
            elif isinstance(msg, SubscribeWithLogic):
                self.subscribe(msg, sender)
            elif isinstance(msg, DeSubscribe):
                subscription = msg.payload
                self.desubscribe(subscription)
        except Exception:
            handle_actor_system_fail()
