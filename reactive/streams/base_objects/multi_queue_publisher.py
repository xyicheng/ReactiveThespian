'''
Maintains multiple queues which are published to. Back pressure occurs
by taking the min of all of the queues being filled.

Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.publisher import Publisher
from reactive.error.handler import handle_actor_system_fail


class MultiQPublisher(Publisher):
    """
    A multi queue publisher maintaining a queue per publisher.
    """


    def __init__(self):
        super().__init__()
        self.__queues = {}

    def on_push(self):
        pass

    def on_pull(self):
        pass

    def receiveMessage(self, msg, sender):
        super().receiveMessage(self, msg, sender)
        try:
            pass
        except Exception:
            handle_actor_system_fail()
