'''
Created on Oct 30, 2017

@author: aevans
'''

from multiprocessing import Queue, cpu_count
from reactive.routers.pub_sub import PubSub
from reactive.error.handler import handle_actor_system_fail
from reactive.routers.round_robin import RoundRobinRouter
from reactive.message.stream_messages import Pull, Push, Complete
from reactive.message.router_messages import Subscribe, DeSubscribe


class RouterStage(PubSub):

    def __init__(self, routee_type, num_workers=cpu_count(),
                 router_class = RoundRobinRouter):
        """
        Constructor

        :param routee_type: Router type
        :type routee_type: Actor
        :param router_class: The router to use
        :type router_class: PubSub
        """
        super().__init__()
        self.__work_router = self.createActor(router_class)
        self.create_workers()
        self.__result_q = Queue(maxsize=1000)
        self.create_workers(routee_type, num_workers)

    def create_workers(self, routee_type, num_workers):
        """
        Create workers for the router

        :param routee_type: The type of router
        :type routee_type: PubSub
        """
        for i in range(0, num_workers):
            worker = self.createActor(routee_type)
            sub = Subscribe(worker, self.__work_router, self)
            self.send(self.__work_router, sub)

    def handle_push(self, msg):
        """
        Add work to the work queue

        :param msg: The Push message
        :type msg: Message
        """
        self.send(self.__work_router, msg)

    def handle_complete(self, msg):
        """
        Handle completed tasks

        :param msg: The completion message
        :type msg: Message
        """
        batch = msg.payload
        if self.__result_q.full():
            if self.drop_policy.lower().strip() == "pop":
                self.__result_q.get_nowait()

        if isinstance(batch, list) is False:
            batch = [batch]
            
        for res in batch:
            if self.__result_q.full() is False:
                self.__result_q.put(res, timeout=120)

    def handle_pull(self, msg):
        """
        Get results and push to sender

        :param msg: Pull message:
        :type msg: Message
        """
        batch_size = msg.payload
        i = 0
        batch = []
        while i < batch_size and self.__result_q.empty() is False:
            res = self.__result_q.get_nowait()
            batch.append(res)
        psh = Push(batch, msg.sender, self)
        self.send(msg.sender, psh)

    def receiveMessage(self, msg, sender):
        """
        Handle the received message

        :param msg: The message
        :type msg: Message
        :param sender: The sender
        :type sender: Actor
        """
        try:
            if isinstance(msg, Pull):
                self.handle_pull(msg)
            elif isinstance(msg, Complete):
                self.handle_complete(msg)
            elif isinstance(msg, Push):
                self.handle_push(msg)
            elif isinstance(msg, Subscribe):
                self.on_subscribe(msg.payload)
            elif isinstance(msg, DeSubscribe):
                self.de_subscribe(msg.payload)
        except Exception:
            handle_actor_system_fail()
