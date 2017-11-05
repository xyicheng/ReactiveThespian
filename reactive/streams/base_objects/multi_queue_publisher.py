'''
Maintains multiple queues which are published to. Back pressure occurs
by taking the min of all of the queues being filled.

Created on Nov 2, 2017

@author: aevans
'''

from queue import Queue
from random import Random
from reactive.streams.base_objects.publisher import Publisher
from reactive.error.handler import handle_actor_system_fail
from reactive.streams.base_objects.subscription import Subscription
from reactive.routers.router_type import RouterType
from reactive.message.stream_messages import Push, Pull


class MultiQPublisher(Publisher):
    """
    A multi queue publisher maintaining a queue per publisher.
    """


    def __init__(self):
        super().__init__()
        self.__queues = {}
        self.__router_type = RouterType.BROADCAST
        self.__current_index = 0

    def set_router_type(self, msg, sender):
        """
        Set the router type

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        payload = msg.payload
        self.__router_type = payload

    def put_in_queue(self, queue, val):
        """
        Put a value on the queue

        :param queue: The queue to put the value on
        :type queue: Queue
        :param val: The value to put on the queue
        :type val: object
        """
        if queue.full():
            if self.drop_policy == "pop":
                try:
                    queue.get_nowait()
                except:
                    pass
        if queue.full() is False:
            queue.put_nowait(val)

    def on_push(self, msg, sender):
        """
        Handle a push request.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender BaseActor
        """
        payload = msg.payload
        if self.__router_type == RouterType.BROADCAST:
            for k,v in self.__queues.items():
                self.put_in_queue(v, payload)
        elif self.__router_type == RouterType.ROUND_ROBIN:
            keys = list(sorted(self.__queues.keys()))
            key = keys[self.__current_index]
            rq = self.__queues[key]
            self.put_in_queue(rq, payload)
        elif self.__router_type == RouterType.RANDOM:
            keys = list(sorted(self.__queues.keys()))
            k = Random().choice(keys)
            rq = self.__queues[rq]
            self.put_in_queue(rq, payload)

    def on_pull(self, msg, sender):
        """
        Handle a pull message.

        :param msg: The Message to handle
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        batch_size = msg.payload
        batch = []
        pull_size = 0
        if sender in self.__queues.keys():
            rq = self.__queues[sender]
            if isinstance(batch_size, int):
                if len(batch_size) > 0:
                    while pull_size < batch_size and rq.empty() is False:
                        val = rq.get_nowait()
                        batch.append(val)
        msg = Push(batch, self.__publisher, self)
        self.send(self.__publisher, msg)
        if pull_size > 0:
            msg = Pull(pull_size, self.__publisher, self)
            self.send(self.__publisher, msg)

    def subscribe(self, subscription):
        """
        Add a subscription and queue.
        """
        if isinstance(subscription, Subscription):
            if subscription not in self.__queues:
                self.__queues[subscription] = Queue()

    def receiveMessage(self, msg, sender):
        super().receiveMessage(self, msg, sender)
        try:
            pass
        except Exception:
            handle_actor_system_fail()
