'''
Created on Oct 29, 2017

@author: aevans
'''

from reactive.message.base_message import Message


class RouteTell(Message):
    pass


class RouteAsk(Message):

    def __init__(self, payload, target, sender, timeout=None):
        super().__init__(payload, target, sender)
        self.timeout = timeout

class BalancingAsk(RouteAsk):

    def __init__(self, router, msg):
        super().__init__(msg.payload, msg.target, msg.sender)
        if isinstance(msg, RouteAsk):
            super().timeout = msg.timeout
        self.router = router

class BalancingTell(RouteTell):

    def __init__(self, router, msg):
        super().__init__(msg.payload, msg.target, msg.sender)
        self.router = router

class Broadcast(Message):
    pass


class Subscribe(Message):
    pass


class DeSubscribe(Message):
    pass


class GetNumRoutees(Message):
    """
    Mostly for testing purposes.
    """
    pass
