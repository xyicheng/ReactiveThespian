'''
A balancing publisher maintains a single queue.  This queue is used to publish
on request.

Created on Nov 2, 2017

@author: aevans
'''

from reactive.streams.base_objects.publisher import Publisher
from queue import Queue
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull, Push, SetDropPolicy

class BalancingPublisher(Publisher):
    """
    Single queue balancing publisher
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.queue = Queue()
        self.__drop_policy = "ignore"
        self.__publisher = None

    def set_drop_policy(self, msg, sender):
        payload = msg.payload
        if isinstance(payload, str):
            self.__drop_policy = payload

    def on_push(self, msg, sender):
        """
        Handle the push message.

        :param msg: The message to handle on push
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        payload = msg.payload
        if isinstance(payload, list):
            for res in payload:
                if self.queue.full() is True:
                    if self.__drop_policy == "pop":
                        try:
                            self.queue.get_nowait()
                        except Exception:
                            handle_actor_system_fail()
                if self.queue.full() is False:
                    self.queue.put_nowait(res)

    def on_pull(self, msg, sender):
        """
        Handle a pull request

        :param msg: The message to send
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        sender = msg.sender
        batch = []
        batch_size = msg.payload
        pull_size = 0
        while self.queue.qsize() > 0 and pull_size < batch_size:
            val = self.queue.get_nowait()
            batch.append(val)
            pull_size += 1
        msg = Push(batch, sender, self)
        self.send(sender, msg)
        msg = Pull(pull_size, self.__publisher, self)
        self.send(self.__publisher, msg)

    def receiveMessage(self, msg, sender):
        """
        Handle a message receipt

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        super().receiveMessage(self, msg, sender)
        try:
            if isinstance(msg, Pull):
                self.on_pull(msg, sender)
            elif isinstance(msg, Push):
                self.on_push(msg, sender)
            elif isinstance(msg, SetDropPolicy):
                self.set_drop_policy(msg, sender)
        except Exception:
            handle_actor_system_fail()
