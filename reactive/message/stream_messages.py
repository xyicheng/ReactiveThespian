'''
Created on Oct 30, 2017

@author: aevans
'''

from reactive.message.base_message import Message


class SubscribeWithLogic(Message):

    def __init__(self, payload, logic, target, sender):
        super().__init__(payload, target, sender)
        self.logic = logic


class SetSubscriber(Message):
    pass


class SetDropPolicy(Message):
    pass


class Pull(Message):
    pass


class PullBySubscription(Message):

    def __init__(self, payload, subscription, target, sender):
        super().__init__(payload, target, sender)
        self.subscription = subscription


class Push(Message):
    pass


class Complete(Message):
    pass


class Cancel(Message):
    pass


class SetPublisher(Message):
    pass
