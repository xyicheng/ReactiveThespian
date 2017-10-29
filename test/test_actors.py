'''
Created on Oct 29, 2017

@author: aevans
'''

import unittest
import logging
from thespian.actors import Actor, ActorSystem
from reactive.message.base_message import Message


class SourceMessage(Message):
    pass


class TargetMessage(Message):
    pass


class TestSource(Actor):

    def receiveMessage(self, msg, sender):
        if isinstance(msg, TargetMessage):
            msg = "Received Message From {} via {}".format(msg, sender)
            logging.info(msg)
        elif isinstance(msg, SourceMessage):
            if msg.sender is None:
                msg.sender = self
            self.send(msg.target, msg)


class TestTarget(Actor):

    def receiveMessage(self, message, sender):
        if isinstance(message, SourceMessage):
            self.send(sender, 'Received a Message {}'.format(message.payload))
        else:
            logging.info("Received {}".format(message))


class TestActorSystem(unittest.TestCase):

    def test_tell(self):
        source = ActorSystem().createActor(TestSource)
        targ = ActorSystem().createActor(TestTarget)
        msg = SourceMessage("Hello From Source", target=targ, sender=source)
        logging.info("Sending {}".format(str(msg)))
        ActorSystem().tell(source, msg)
        ActorSystem().shutdown()

    def test_ask(self):
        pass

    def test_round_robin_router(self):
        pass

    def test_balancing_router(self):
        pass


if __name__ == "__main__":
    unittest.main()
