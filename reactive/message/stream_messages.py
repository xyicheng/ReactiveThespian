'''
Created on Oct 30, 2017

@author: aevans
'''

from reactive.message.base_message import Message


class SubscribeWithLogic(Message):

    def __init__(self, payload, logic, target, sender):
        super().__init__(payload, target, sender)
        self.logic = logic


class SubscribeWithPriority(Message):

    def __init__(self, payload, priority, target, sender):
        super().__init__(payload, target, sender)
        self.priority = priority


class ResetPriority(Message):

    def __init__(self, payload, priority, target, sender):
        super().__init__(payload, target, sender)
        self.priority = priority


class Peek(Message):
    pass


class SetSubscriber(Message):
    pass


class SetDropPolicy(Message):
    pass


class GetDropPolicy(Message):
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


class GetSubscribers(Message):
    pass


class GetPublisher(Message):
    pass
