'''
Created on Oct 31, 2017

@author: aevans
'''

import pytest
from thespian.actors import ActorSystem
from reactive.streams.sink import Sink


class PrintSink(Sink):
    """
    Print Sink
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__()

    def on_push(self, msg, sender):
        """
        Handle the push

        :param msg: The message to handle
        :type msg: Message
        :param sender: The sender
        :type sender: Actor
        """
        payload = msg.payload
        print(payload)


@pytest.fixture(scope="module")
def asys():
    """
    Create an Actor System
    """
    asys = ActorSystem()
    yield asys
    asys.shutdown()


class TestSink():

    def test_setup(self, asys):
        sink = asys.createActor(PrintSink)

    def test_source_sink_connection(self, asys):
        sink = asys.createActor(PrintSink)

    def test_push(self):
        pass

    def test_multi_push(self):
        pass
