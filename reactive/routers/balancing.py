'''
A router in which all actors receive work from a commonly shared queue.

Created on Oct 29, 2017

@author: aevans
'''

from queue import Queue
from reactive.routers.pub_sub import PubSub
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import RouteTell, RouteAsk, Subscribe,\
    DeSubscribe, Broadcast, BalancingAsk, BalancingTell
from reactive.message.base_message import Message


class BalancingRouter(PubSub):
    """
    Needs work. Attempts to balance tasks/router combination.
    At the moment, there is an issue where messages must be sent by a caller
    or routee in order for the queue to be checked. Moving fast so this may
    need to be reworked. It would be better to dig into Thespian and find a
    way to replace the typical queue used as a mailbox with a shared mailbox.
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.__actor_task_q = Queue(maxsize=1000)
        self.__actor_ready_q = Queue(maxsize=1000)

    def check_and_send_task(self):
        """
        Check and send a message if an actor is ready.
        """
        if self.__actor_task_q.empty() is False:
            if self.__actor_task_q.empty() is False:
                actor = self.__actor_ready_q.get_nowait()
                task_message = self.__actor_task_q.get_nowait()
                self.tell(actor, task_message)

    def handle_message(self, msg, sender):
        """
        Handle an incoming ask message.

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        sender = msg.sender
        if sender in self.__actor_set:
            if self.__actor_ready_q.full() is False:
                self.__actor_ready_q.put_nowait(sender)
            self.check_and_send_task()
        else:
            bmsg = None
            if isinstance(msg, RouteAsk):
                bmsg = BalancingAsk(self, msg)
            else:
                bmsg = BalancingTell(self, msg)
            self.__actor_task_q.put_nowait(bmsg)
            self.check_and_send_task()

    def receiveMessage(self, msg, sender):
        """
        Handle the incoming messages

        :param msg: The message to handle
        :type msg: Message()
        :param sender: The message sender
        :type sender: Actor()
        """
        try:
            self.check_message_and_sender(msg, sender)
            if isinstance(msg, RouteTell):
                self.handle_tell(msg, sender)
            elif isinstance(msg, RouteAsk):
                self.handle_ask(msg, sender)
            elif isinstance(msg, Subscribe):
                self.handle_subscription(msg, sender)
            elif isinstance(msg, DeSubscribe):
                self.handle_desubscribe(msg, sender)
            elif isinstance(msg, Broadcast):
                self.handle_broadcast(msg, sender)
            else:
                err_msg = "PubSub Does not Understand {}\n{}"
                err_msg = err_msg.format(str(type(msg)), str(self))
                raise ValueError(err_msg)
        except:
            handle_actor_system_fail()
