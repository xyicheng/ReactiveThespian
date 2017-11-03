'''
Subscription sends requests for information. It takes an actor to send to
as a reference and passes batch requests to it via Pull(n, rec, sender).
This is both a subscriber and a subscription per the reactive streams
model. Not buying into the idea of have one subscription and a separate
subscriber.

Created on Nov 1, 2017

@author: aevans
'''

import logging
from queue import Queue
from thespian.actors import ActorExitRequest

from reactive.actor.base_actor import BaseActor
from reactive.actor.state import ActorState
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import Subscribe
from reactive.message.stream_messages import Pull, Cancel, Push, SetSubscriber,\
    SetDropPolicy


class Subscription(BaseActor):
    """
    Subscription router
    """

    def __init__(self):
        """
        Constructor

        :param subscriber: The subscriber actor
        :type subscriber: BaseActor
        :param drop_policy: Policy to take if the results queue
        :type drop_policy
        """
        super().__init__()
        self.__result_q = Queue(maxsize=1000)
        self.__subscriber = None
        self.__subscribed = False
        self.__drop_policy = "ignore"

    def set_drop_policy(self, msg, sender):
        """
        Set the drop policy.

        :param msg: The message containing the policy
        :type msg: Message
        :param sender: The sender
        """
        payload = msg.payload
        if isinstance(payload, str):
            if payload.strip().lower() in ["pop", "ignore"]:
                self.__drop_policy = payload

    def set_subscriber(self, msg, sender):
        """
        Set the subscriber

        :param msg: The message with the subscriber
        :type msg: Message
        :param sender: The subscription sender
        :type sender: BaseActor
        """
        payload = msg.payload
        if isinstance(msg, BaseActor):
            self.__subscriber = payload

    def request(self, batch_size, sender):
        """
        Send out a request for a batch of work

        :param batch_size: The batch size
        :type batch_size: int()
        :param sender: The sender of the original message
        :type sender: BaseActor
        """
        rec = self.__subscriber
        msg = Pull(batch_size, self)
        self.send(rec, msg)

    def on_next(self, batch_size, sender):
        """
        On the next request, call request

        :param batch_size: The batch size
        :type batch_size: int()
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        pull_size = 0
        batch = []
        while pull_size < batch_size and self.__result_q.empty() is False:
            val = self.__result_q.get_nowait()
            batch.append(val)
            pull_size += 1
        msg = Push(batch, sender, self)
        self.send(sender, msg)
        if pull_size > 0:
            self.request(pull_size, sender)
            self.handle_next(sender)

    def cancel(self):
        """
        Set the state to down. Kill the actor with ActorExitRequest
        """
        self.on_complete()
        self.send(self, ActorExitRequest())
        self.state = ActorState.TERMINATED

    def on_subscribe(self, msg, sender):
        """
        This subscribes to a publisher. Only one publisher is allowed.


        :param msg: The sending message
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        if self.__subscribed is False:
            pub = msg.payload
            if isinstance(pub, BaseActor):
                Subscribe(self.__subscriber, pub, self)
                self.send(pub, msg)
                self.__subscribed = True

    def on_complete(self):
        """
        User implemented. Called on Cancel. Will not be called
        on a different form of error that causes catastrophic
        failure.
        """
        logging.info("Actor Completing {}".format(str(self)))
    
    def handle_batch(self, msg):
        """
        Handle a batch of results.

        :param msg: The message to handle
        :type msg: Message
        """
        batch = msg.payload
        if isinstance(batch, list):
            for result in batch:
                if self.__result_q.full():
                    if self.__drop_policy == "pop":
                        try:
                            self.__result_q.get_nowait()
                        except Exception:
                            pass

                if self.__result_q.full() is False:
                    self.__result_q.put(result)

    def receiveMessage(self, msg, sender):
        """
        Handle a message on receipt. The message sender is used to
        receive the results of any additional request

        :param msg: The message received
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        try:
            if isinstance(msg, Pull):
                batch_size = msg.payload
                self.on_next(batch_size, msg.sender)
            elif isinstance(msg, Push):
                self.handle_batch(msg)
            elif isinstance(msg, SetSubscriber):
                self.set_subscriber(msg, sender)
            elif isinstance(msg, SetDropPolicy):
                self.set_drop_policy(msg, sender)
            elif isinstance(msg, Cancel):
                self.cancel()
        except Exception:
            handle_actor_system_fail()
