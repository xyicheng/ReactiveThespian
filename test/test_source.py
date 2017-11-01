'''
Created on Oct 31, 2017

@author: aevans
'''

import asyncio
import re
import pytest
from reactive.streams.source import Source
from thespian.actors import ActorSystem
from reactive.message.stream_messages import Pull, Push


class SourceTest(Source):

    def __init__(self):
        super().__init__()
        self.__index = 0

    def on_pull(self, msg):
        return "Received {}".format(self.__index)


@pytest.fixture(scope="module")
def asys():
    asys = ActorSystem()
    yield asys
    asys.shutdown()


class TestSource():

    def test_startup(self, asys):
        """
        Test the startup
        """
        source = asys.createActor(SourceTest)

    async def do_pull(self, asys):
        """
        Async def for pulling.
        """
        source = asys.createActor(SourceTest)
        pull = Pull(3, source, source)
        rval = asys.ask(source, pull, timeout=120)
        assert(rval, str)
        return rval

    def test_pull(self, asys):
        """
        Test a pull
        """
        source = asys.createActor(SourceTest)
        pull = Pull(1, source, source)
        rval = asys.ask(source, pull, timeout=120)
        assert isinstance(rval, Push)
        assert len(rval.payload) > 0
        assert re.match("Received\s+\d+", rval.payload[0])

    def test_multi_pull(self, asys):
        """
        Test multiple pull requests at once
        """
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(asyncio.gather(*[
            self.do_pull(asys),
            self.do_pull(asys)]))
        for result in results:
            assert len(result.payload) == 3
            for rval in result.payload:
                assert isinstance(rval, str) 
                assert re.match("Received\s+\d+", rval)
