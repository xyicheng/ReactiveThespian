'''
Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.publisher import Publisher
from queue import Queue

class BalancingPublisher(Publisher):

    def __init__(self):
        super().__init__()
        self.queue = Queue()

    def on_push(self, msg, sender):
        pass

    def on_pull(self, msg, sender):
        pass

    def receiveMessage(self, msg, sender):
        super().receiveMessage(self, msg, sender)
