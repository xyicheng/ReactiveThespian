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


class Broadcast(Message):
    pass


class Subscribe(Message):
    pass


class DeSubscribe(Message):
    pass
