'''
Test a balancing publisher.

Created on Nov 7, 2017

@author: aevans
'''

from datetime import datetime, timedelta
import pytest 
from thespian.actors import ActorSystem
from reactive.streams.base_objects.balancing_publisher import BalancingPublisher
from reactive.streams.base_objects.subscription import Subscription
from reactive.message.router_messages import Subscribe
from test.modules.object_work import get_work_batch_array
from reactive.message.stream_messages import Push, Pull, SetPublisher,\
    SetSubscriber
import pdb
from test.modules.base_object_actors import PublisherStringActor, SubTest,\
    PublisherArrayActor
from reactive.streams.base_objects.rated_subscription_pool import RatedSubscriptionPool
from time import sleep


@pytest.fixture(scope="module")
def asys():
    asys = ActorSystem()
    yield asys
    asys.shutdown()


class TestBalancingPublisher():

    def test_creation(self, asys):
        """
        Test a publisher creation
        """
        pub = asys.createActor(BalancingPublisher)

    def test_push_pull(self, asys):
        """
        Test the push and pull
        """
        pub = asys.createActor(BalancingPublisher)
        pubw = asys.createActor(PublisherArrayActor)
        msg = SetPublisher(pubw, pub, None)
        asys.tell(pub, msg)
        msg = Pull(5, pub, None)
        rval =  asys.ask(pub, msg)
        tstart = datetime.now()
        while (rval.payload is None or len(rval.payload) is 0)\
        and tstart - datetime.now() <  timedelta(seconds=120):
            pll = Pull(50, pub, None)
            rval = asys.ask(pub, pll)
        assert isinstance(rval, Push)
        assert isinstance(rval.payload, list)
        assert len(rval.payload) == 4

    def stest_push_pull_with_pub(self, asys):
        """
        Test the push and pull
        """
        pub = asys.createActor(BalancingPublisher)
        spa = asys.createActor(PublisherStringActor)
        msg = SetPublisher(spa, pub, None)
        asys.tell(pub, msg)
        rr = asys.createActor(RatedSubscriptionPool)

        suba = asys.createActor(Subscription)
        ssn = SetSubscriber(pub, suba, None)
        asys.tell(suba, ssn)
        rrs = Subscribe(suba, rr, None)
        asys.tell(rr, rrs)

        st = asys.createActor(SubTest)
        msg = Subscribe(rr, st, None)
        asys.tell(st, msg)

        pll = Pull(50, rr, None)
        rval = asys.ask(st, pll)
        assert isinstance(rval, Push)
        assert isinstance(rval.payload, list)
        assert len(rval.payload) is 0
        tstart = datetime.now()
        i = 0
        while i < 100 and tstart - datetime.now() <  timedelta(seconds=120):
            pll = Pull(50, st, None)
            rval = asys.ask(st, pll)
            if len(rval.payload) > 0:
                i += 1
        assert i == 100
        assert len(rval.payload) is 50

    def test_push_pull_with_multi_pub(self, asys):
        """
        Test the push and pull
        """
        pubb = asys.createActor(BalancingPublisher)
        spab = asys.createActor(PublisherStringActor)
        msg = SetPublisher(spab, pubb, None)
        asys.tell(pubb, msg)
        pub = asys.createActor(BalancingPublisher)
        spa = asys.createActor(PublisherStringActor)
        msg = SetPublisher(spa, pub, None)
        asys.tell(pub, msg)

        rr = asys.createActor(RatedSubscriptionPool)

        suba = asys.createActor(Subscription)
        ssn = SetSubscriber(pub, suba, None)
        asys.tell(suba, ssn)
        subb = asys.createActor(Subscription)
        ssn = SetSubscriber(pub, subb, None)
        asys.tell(subb, ssn)

        rrs = Subscribe(suba, rr, None)
        asys.tell(rr, rrs)

        st = asys.createActor(SubTest)
        msg = Subscribe(rr, st, None)
        asys.tell(st, msg)

        pll = Pull(50, rr, None)
        rval = asys.ask(st, pll)
        assert isinstance(rval, Push)
        assert isinstance(rval.payload, list)
        assert len(rval.payload) is 0
        tstart = datetime.now()
        i = 0
        while i < 10 and tstart - datetime.now() <  timedelta(seconds=120):
            pll = Pull(500, st, None)
            rval = asys.ask(st, pll)
            if len(rval.payload) > 0:
                i += 1
        assert i == 10
        assert len(rval.payload) == 500
