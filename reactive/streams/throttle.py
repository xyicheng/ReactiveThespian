'''
Throttle inputs and outputs. Output a maximum 
count. Backpressure should propogate up the stage.

Created on Nov 1, 2017

@author: aevans
'''

from queue import Queue
from reactive.routers.pub_sub import PubSub
from multiprocessing import cpu_count
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Push, Pull


class ThrottleStage(PubSub):
    """
    A throttle stage pushing only a certain number of results on call. The
    number of records pushed is min(request, throttle_size).
    """

    def __init__(self, throttle_size=cpu_count(), drop_policy="ignore"):
        """
        Constructor

        :param throttle_size: The number of items to push on request
        :type throttle_size: int()
        :param drop_policy: The type of policy to follow on drop
        :type drop_policy: str()
        """
        super().__init__()
        self.__output_size = throttle_size
        self.__result_q = Queue(maxsize = 1000)
        self.__drop_policy = drop_policy

    def receiveMessage(self, msg, sender):
        """
        Handle a message.

        :param msg: The message to handle.
        :type msg: Message()
        :param sender: The sender of the message
        :type sender: BaseActor()
        """
        super().receiveMessage(self, msg, sender)
        try:
            if isinstance(Push):
                batch = msg.payload
                if isinstance(batch, list):
                    batch_size = len(batch)
                    for i in range(0, batch_size):
                        work = batch[i]
                        if self.__result_q.full():
                            if self.__drop_policy == "pop":
                                try:
                                    self.__result_q.get_nowait()
                                except:
                                    pass
                        if self.__result_q.empty() is False:
                            self.result__q.put_nowait(work)
            elif isinstance(Pull):
                if self.__result_q.empty() is False:
                    sender = msg.sender
                    batch_size = msg.payload
                    batch = []
                    sizes = [
                            self.__output_size,
                            batch_size
                        ]
                    batch_size = min(sizes)
                    for i in range(0, batch_size):
                        if self.__result_q.empty() is False:
                            res = self.__result_q.get_nowait()
                            batch.append(res)
                    psh = Push(batch, sender)
                    self.send(sender, psh)
        except Exception:
            handle_actor_system_fail()
