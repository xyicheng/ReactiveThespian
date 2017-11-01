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
from reactive.message.stream_messages import Pull, Cancel, Push


class Subscription(BaseActor):
    """
    Subscription router
    """

    def __init__(self, subscriber, drop_policy="ignore"):
        """
        Constructor

        :param subscriber: The subscriber actor
        :type subscriber: BaseActor
        :param drop_policy: Policy to take if the results queue overfills
        :type drop_policy
        """
        super().__init__()
        self.__result_q = Queue(maxsize=1000)
        self.__subscriber = subscriber
        self.__subscribed = False
        self.__drop_policy = drop_policy
        self.check_setup()

    def check_setup(self):
        """
        Check the actor setup.
        """
        if self.__subscriber is None or\
        isinstance(self.__subscriber, BaseActor) is False:
            err_msg = "Subscription Requires Actor, Received {}"
            err_msg = err_msg.format(str(self.__subscriber))
            logging.error(err_msg)
            raise ValueError(err_msg)
        if self.__drop_policy is None or\
        self.__drop_policy not in ["ignore", "pop"]:
            err_msg = "Drop policy must be pop or ignore in {}"
            err_msg = err_msg.format(str(self))
            logging.error(err_msg)
            raise ValueError(err_msg)

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

    def handle_next(self, sender):
        """
        User implemented. Called when a request is made.
        """
        None

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
        do_loop = True
        while do_loop:
            if self.__result_q.empty() is False:
                val = self.__result_q.get_nowait()
                batch.append(val)
                pull_size += 1
            else:
                do_loop = False
        if pull_size > 0:
            msg = Push(pull_size, sender, self)
        self.send(sender, msg)
        self.request(pull_size, sender)
        self.handle_next(sender)

    def cancel(self):
        """
        Set the state to down. Kill the actor with ActorExitRequest
        """
        self.on_complete()
        self.send(self, ActorExitRequest())
        self.state = ActorState.TERMINATED

    def handle_subscribe(self):
        """
        Created by the user. Called on subscription to a publisher only once.
        If the subscription is placed in multiple publishers it will not be
        called again per the reactive streams model.
        """
        logging.info("Publisher subscribed {}".format(str(self)))

    def on_subscribe(self, msg, sender):
        """
        Should be called when the subscription is subscribed to a publisher.
        This method is called at most once.

        :param msg: The sending message
        :type msg: Message
        :param sender: The sender
        :type sender: BaseActor
        """
        if not self.__subscribed:
            try:
                self.handle_subscribe()
            except Exception:
                handle_actor_system_fail()
            self.__subscribed = True

    def on_complete(self):
        """
        User implemented. Called on Cancel. Will not be called
        on a different form of error that causes catastrophic
        failure.
        """
        logging.info("Actor Completing {}".format(str(self)))

    def on_push(self):
        """
        Handle a push to this subscription from a publisher.
        """
        pass
    
    def handle_batch(self, msg):
        """
        Handle a batch of results.
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
            elif isinstance(msg, Cancel):
                self.cancel()
        except Exception:
            handle_actor_system_fail()
