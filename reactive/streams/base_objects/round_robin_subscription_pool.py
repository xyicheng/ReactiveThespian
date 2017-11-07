'''
Implements SubscriptionPool but uses round robin on subscriptions when calling
get_next.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull, Push
from reactive.streams.base_objects.subscription_pool import SubscriptionPool

class RoundRobinSubscriptionPool(SubscriptionPool):
    """
    Round robin based subscription pool.
    """
        
    def __init__(self):
        """
        Constructor

        :param drop_policy: Specify how to handle queue overfill
        :type drop_policy: str()
        """
        super().__init__()
        self.__index = 0

    def next(self, msg, sender):
        """
        Get the next n elements in the batch.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        batch_size = msg.payload
        batch = []
        rq = super().get_result_q()
        pull_size = 0
        if batch_size > 0:
            if rq.empty() is False:
                i = 0
                while rq.empty() is False and i < batch_size:
                    try:
                        pull_size += 1
                        val = rq.get_nowait()
                        batch.append(val)
                    except Exception:
                        handle_actor_system_fail()
                    finally:
                        i += 1
        msg = Push(batch, sender, self)
        self.send(sender, msg)
        subs = self.get_subscriptions()
        if pull_size > 0:
            if subs and len(subs) > 0:
                if self.__index >= len(subs):
                    self.__index = 0
                sub = subs[self.__index]
                self.__index += 1
                msg = Pull(pull_size, sub, self.myAddress)
                self.send(sub, msg)
        elif rq.empty() and len(subs) > 0:
            def_q_size = self.get_default_queue_size()
            pull_size = int(def_q_size / len(subs))
            for sub in subs:
                msg = Pull(pull_size, sub, self.myAddress)
                self.send(sub, msg)
