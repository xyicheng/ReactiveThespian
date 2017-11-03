'''
A Logic processor stores a queue per condition. A default base queue is
supported as well.

Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.publisher import Publisher
from queue import Queue


class LogicPublisher(Publisher):

    def __init__(self):
        self.__init__()
        self.__queues ={
            'default' : Queue()
        }

    def on_push(self, msg, sender):
        pass

    def on_pull(self, msg, sender):
        pass

    def receiveMessage(self, msg, sender):
        super().receiveMessage(self, msg, sender)
