'''
Implements SubscriptionPool but uses round robin on subscriptions when calling
get_next.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.streams.base_objects.subscription_pool import SubscriptionPool
from reactive.message.stream_messages import Pull, Push
from reactive.error.handler import handle_actor_system_fail


class RoundRobinSubscriptionPool(SubscriptionPool):
    """
    Round robin based subscription pool.
    """
    
    
    def __init__(self, drop_policy="ignore"):
        """
        Constructor

        :param drop_policy: Specify how to handle queue overfill
        :type drop_policy: str()
        """
        super().__init__(drop_policy)
        self.__index = 0

    def next(self, msg):
        """
        Get the next n elements in the batch.

        :param msg: The message to handle
        :type msg: Message
        """
        sender = msg.sender
        subs = super().__subscriptions
        batch_size = msg.payload
        batch = []
        rq = super().__result_q
        if rq.empty() is False:
            i = 0
            while rq.empty() is False and i < batch_size():
                try:
                    val = rq.get_nowait()
                    batch.append(val)
                except Exception:
                    handle_actor_system_fail()
                finally:
                    i += 1
        msg = Push(batch, sender, self)
        self.send(sender, msg)

        if subs and len(subs) > 0:
            pull_size = 1000 - rq.qsize()
            sub = subs[self.__index]
            msg = Pull(pull_size, sub, self)
            self.send(sub, msg)
