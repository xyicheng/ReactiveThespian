'''
Created on Oct 30, 2017

@author: aevans
'''

from reactive.message.base_message import Message


class SetSubscriber(Message):
    pass


class SetDropPolicy(Message):
    pass


class Pull(Message):
    pass


class Push(Message):
    pass


class Complete(Message):
    pass


class Cancel(Message):
    pass
