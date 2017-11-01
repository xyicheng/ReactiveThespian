'''
Created on Nov 1, 2017

@author: aevans
'''

from multiprocessing import Queue
from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.stream_messages import Pull, Push
from thespian.actors import PoisonMessage


class QueuedSource(BaseActor):

    def __init__(self, drop_policy="ignore", buffer_size=1000):
        super().__init__()
        self.__drop_policy = drop_policy
        self.__result_q = Queue(maxsize=buffer_size)
        self.__work_q = Queue(maxsize=buffer_size)
        self.__buffer_size = buffer_size

    def on_pull(self, work):
        """
        Implement a pull request.

        :param work: The work to handle
        :type work: Message
        """
        return None

    def pull_proc(self, drop_policy, pull_func):
        """
        Do not overwrite. The individual pull process.

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        do_loop = True
        while do_loop: 
            try:
                work = self.__work_q.get(timeout=120)
                if isinstance(work, PoisonMessage) is False:
                    if self.__result_q.full() is False:
                        result = pull_func(work)
                        self.__result_q.put_nowait(result)
                else:
                    do_loop = False
            except Exception:
                handle_actor_system_fail()

    def receiveMessage(self, msg, sender):
        """
        Handle a message on receipt.

        :param msg: The incomming message
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        try:
            if isinstance(msg, Pull):
                sender = msg.sender
                batch_size = msg.payload
                batch = []
                if batch_size > 0:
                    for i in range(0, batch_size):
                        if self.__result_q.empty() is False:
                            batch_val = self.__result_q.get_nowait()
                            batch.append(batch_val)
                qsize = self.__work_q.qsize()
                pull_size = self.__buffer_size - qsize
                if pull_size > 0:
                    for i in range(0, pull_size):
                        if self.__work_q.full() is True:
                            if self.__drop_policy =="pop":
                                try:
                                    self.__work_q.get_nowait()
                                except:
                                    pass

                        if self.__work_q.full() is False:
                            self.__work_q.put_no_wait()
                psh = Push(batch, sender)
                self.send(psh, sender)
        except Exception:
            handle_actor_system_fail()
