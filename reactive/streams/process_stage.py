'''
Created on Oct 29, 2017

@author: aevans
'''

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue
from reactive.routers.pub_sub import PubSub
from reactive.message.stream_messages import Push, Pull
from reactive.message.router_messages import Subscribe, DeSubscribe
import traceback
from reactive.error.handler import handle_actor_system_fail


class ProcessStage(PubSub):
    """
    Stage PubSub. A stage handles a work step. The push function
    is overwritten and provided with work 
    """

    def __init__(
            self, num_workers = 1, max_batch_size=16, drop_policy="ignore",
            executor = ProcessPoolExecutor()):
        """
        Constructor

        :param num_workers: Number of worker processes to start
        :type num_workers: int()
        :param max_batch_size: Maximum batch size to execute at once
        :type max_batch_size: int()
        :param drop_policy: The policy to use when a queu is full
        :type drop_policy: str()
        :param executor: executor to submit tasks to
        :type executor: Executor from Futures
        """
        super().__init__()
        self.__work_q = Queue(max_size=1000)
        self.__result_q = Queue(max_size=100)
        self.__executor = executor
        self.max_batch_size = max_batch_size
        self.num_workers = num_workers
        self.drop_policy = drop_policy
        self.create_workers(num_workers)

    def on_pull(self, el):
        """
        Perform work and return values.
        """
        return None

    def worker(self):
        """
        The worker process.
        """
        run = True
        while run:
            try:
                output = None
                res = self.__work_q.get(timeout=120)
                if isinstance(res, list):
                    for el in res:
                        val = self.on_pull(el)
                        self.__result_q.put(val, timeout=120)
                else:
                    val = self.on_pull(res)
                    self.__result_q.put(val, timeout=120)
            except:
                traceback.print_exc()

    def create_workers(self, num_workers):
        """
        Create a set of workrs running in the executor.

        :param num_workers: The number of workers
        :type num_workers: int
        """
        self.__executor.submit(self.worker)

    def handle_push(self, msg, sender):
        """
        Handle a push request

        :param msg: The Push msg
        :type msg: Message
        """
        payload = msg.payload
        if isinstance(payload, list):
            for work in payload:
                if self.__work_q.full():
                    if self.drop_policy.lower().strip() == "pop":
                        self.__work_q.get_nowait()

                if self.__work_q.full() is False:
                    self.__work_q.put(work, timeout=120)
        else:
            if self.__work_q.full():
                if self.drop_policy.lower().strip() == "pop":
                    self.__work_q.get_nowait()
            self.__work_q.put(payload, timeout=120)

    def handle_pull(self, msg, sender):
        """
        Handle the pull request

        :param msg: The Pull message
        :type msg: Message
        :param sender: The sender
        :type sender: Actor
        """
        batch = []
        batch_size = msg.payload
        i = 0 
        while i < batch_size and self.__result_q.empty() is False:
            val = self.__result_q.get_nowait()
            batch.append(val)
        self.send(msg.sender, Pull(batch, sender, self))


    def receiveMessage(self, msg, sender):
        """
        Handle a message on receipt

        :param msg: Message to handle
        :type msg: Message()
        :param sender: Message sender
        :type sender: BaseActor()
        """
        try:
            if isinstance(msg, Push):
                self.handle_push(msg, sender)
            elif isinstance(msg, Pull):
                self.handle_pull(msg, sender)
            elif isinstance(msg, Subscribe):
                self.on_subscribe(msg.payload)
            elif isinstance(msg, DeSubscribe):
                self.de_subscribe(msg.payload)
        except Exception:
            handle_actor_system_fail()
