'''
A processor serves as the basic stage.  The basic processor contains a
Publisher and a SubscriptionPool.

Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.round_robin_subscription_pool import RoundRobinSubscriptionPool
from reactive.actor.base_actor import BaseActor


class ProcessorStage(BaseActor):

    def __init__(self, publisher, routing_logic="round_robin",
                 subscription_pool=RoundRobinSubscriptionPool()):
        """
        Constructor

        :param publisher: The publisher attached to the processor (out)
        :type publisher: Publisher
        :param routing_logic: The type of logic for the publisher
        :type routig_logic: str()
        :param subscription_pool: The subscription pool (in)
        :type subscription_pool: SubscriptionPool
        """
        super().__init__()
        self.publisher = publisher
        self.subscription_pool = subscription_pool
        self.routing_logic = routing_logic
