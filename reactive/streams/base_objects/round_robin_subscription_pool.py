'''
Implements SubscriptionPool but uses round robin on subscriptions when calling
get_next.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.streams.base_objects.subscription_pool import SubscriptionPool


class RoundRobinSubscriptionPool(SubscriptionPool):

    def __init__(self):
        super().__init__()
        self.__index = 0

    def next(self, batch_size):
        subs = super().__subscriptions
        if subs and len(subs) > 0:
            sub = subs[self.__index]
            self.send(sub, msg)
