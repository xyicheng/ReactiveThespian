'''
Throttle inputs and outputs. Output a maximum 
count. Backpressure should propogate up the stage.

Created on Nov 1, 2017

@author: aevans
'''

from multiprocessing import Queue
from reactive.routers.pub_sub import PubSub
from multiprocessing import cpu_count
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Push, Pull
from time import sleep


class TimedThrottleStage(PubSub):
    """
    A throttle stage pushing only a certain number of results on call. The
    number of records pushed is min(request, throttle_size).
    """

    def __init__(self, throttle_size=cpu_count(), drop_policy="ignore",
                 by_batch=False, sleep_time=2):
        """
        Constructor

        :param throttle_size: The number of items to push on request
        :type throttle_size: int()
        :param drop_policy: The type of policy to follow on drop
        :type drop_policy: str()
        :param by_batch: Wait on an entire batch. Results are streamed 1 by 1.
        :type by_batch: bool()
        :param sleep_time: The time to sleep
        :type sleep_time: int()
        """
        super().__init__()
        self.__output_size = throttle_size
        self.__work_q = Queue(maxsize = 1000)
        self.__result_q = Queue(maxsize = 1000)
        self.__drop_policy = drop_policy
        self.__by_batch = by_batch
        self.__sleep_time = sleep_time

    def handle_batch(self, sleep_time):
        """
        Handle a batch.
        """
        try:
            do_loop = True
            while do_loop:
                try:
                    work = self.__work_q.get(timeout=120)
                    sleep(sleep_time)
                    if self.__result_q.full():
                        if self.__drop_policy == "pop":
                            try:
                                self.__result_q.get_nowait()
                            except:
                                pass
                    if self.__result_q.full() is False:
                        self.__result_q.put()
                except Exception:
                    handle_actor_system_fail()
        except Exception:
            handle_actor_system_fail()

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
