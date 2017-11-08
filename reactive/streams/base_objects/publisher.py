'''
A reactive streams oriented Publisher. This is a special router which contains
any routing logic (BROADCAST, ROUND_ROBING, RANDOM; etc.). The router contains
subscriptions instead of routees. Publish requests are sent to subscribers.

Created on Nov 1, 2017

@author: aevans
'''

from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from reactive.message.router_messages import Subscribe, DeSubscribe
from reactive.streams.base_objects.subscription import Subscription
from reactive.message.stream_messages import SetPublisher, SetDropPolicy,\
    GetSubscribers, GetPublisher
import pdb


class Publisher(BaseActor):
    """
    Publisher. Publishes messages to subscribers.
    """

    def __init__(self):
        """
        Constructor

        :param router: The router to use.  All extend PubSub which is default.
        :type router: PubSub
        """
        self.__subscriptions = []
        self.__publisher = None
        self.drop_policy = "ignore"

    def subscribe(self, msg, sender):
        """
        Subscribe a subscription actor

        :param subscription: The subscription to use
        :type subscription: Subscription
        """
        try:
            sub = msg.payload
            self.__subscriptions.append(sub)    
        except Exception:
            handle_actor_system_fail()

    def desubscribe(self, msg, sender):
        """
        DeSubscribe a subscription actor

        :param subscription: The actor to use
        :type subscription: Subscription
        """
        try:
            rsub = msg.payload
            sp = None
            for sub in self.__subscriptions:
                if rsub == sub:
                    sp = sub
            if sp:
                self.__subscriptions.remove(sp)
        except Exception:
            handle_actor_system_fail()

    def set_publisher(self, msg, sender):
        """
        Set the publisher

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        payload = msg.payload
        self.__publisher = payload 

    def set_drop_policy(self, msg, sender):
        """
        Set the drop policy 
        """
        payload = msg.payload
        self.drop_policy = payload

    def get_subscribers(self, msg, sender):
        """
        Obtain the current subscribers

        :param msg: Message to handle
        :type msg: Message
        :param sender: Message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        subs = self.__subscriptions
        msg = GetSubscribers(subs, sender, self.myAddress)
        self.send(sender, msg)

    def get_publisher(self, msg, sender):
        """
        Get the current publisher

        :param msg: The message to handle
        :type msg: Message
        :param sender: The message sender
        :type sender: BaseActor
        """
        if msg.sender:
            sender = msg.sender
        pub = self.__publisher
        msg = GetPublisher(pub, sender, self.myAddress)
        self.send(sender, msg)

    def receiveMessage(self, msg, sender):
        """
        Handle message on receipt.

        :param msg: The message received
        :type msg: Message
        :param sender: The sender of the message
        :type sender: BaseActor
        """
        try:
            if isinstance(msg, Subscribe):
                self.subscribe(msg, sender)
            elif isinstance(msg, DeSubscribe):
                self.desubscribe(msg, sender)
            elif isinstance(msg, SetPublisher):
                self.set_publisher(msg, sender)
            elif isinstance(msg, SetDropPolicy):
                self.set_drop_policy(msg, sender)
            elif isinstance(msg, GetSubscribers):
                self.get_subscribers(msg, sender)
            elif isinstance(msg, GetPublisher):
                self.get_publisher(msg, sender)
        except Exception:
            handle_actor_system_fail()
