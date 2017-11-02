'''
Broadcast for batch size across multiple sources up at 1/subs up to a min_size.
Not all subscriptions will receive requests for work if 1/subs < min_size.

Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.subscription_pool import SubscriptionPool
from multiprocessing import cpu_count
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull


class BroadcastSubscriptionPool(SubscriptionPool):

    def __init__(self, drop_policy="ignore", min_batch_size=cpu_count()):
        super().__init__(drop_policy)
        self.__min_batch_size = min_batch_size
        self.__index = 0

    def next(self, msg):
        """
        Obtain the next set in the batch up to n records.  Send pull requests
        by broadcast style routing up to the batch size as regulated by
        min_size.
        """
        sender = msg.sender
        subs = super().__subscriptions
        batch_size = msg.payload
        batch = []
        rq = super().__result_q
        if rq.empty() is False:
            i = 0
            while i < batch_size and rq.empty() is False:
                val = rq.get_nowait()
                batch.append(val)
        pull_size = 1000 - rq.qsize()
        per_node_size = pull_size / len(super().__subscriptions)
        per_node_size = max([int(per_node_size), self.__min_batch_size])
        remaining = pull_size
        while remaining > 0:
            left = per_node_size
            if left > remaining:
                left = remaining
                sub  = super().__subscriptions[self.__index]
                msg = Pull(left, sub, self)
                self.send(sub, msg)
                self.__index += 1
                if self.__index >= len(super().__subscriptions):
                    self.__index = 0
