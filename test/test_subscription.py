'''
Test the creation and messaging to a subscription.

Created on Nov 2, 2017

@author: aevans
'''


import pytest
from thespian.actors import ActorSystem

from reactive.message.stream_messages import Cancel, Pull, Push
from reactive.streams.base_objects.subscription import Subscription
from reactive.actor.base_actor import BaseActor
from reactive.error.handler import handle_actor_system_fail
from test.modules.actors import StringBatchTestActor


@pytest.fixture(scope="module")
def asys():
    return ActorSystem()


class TestSubscription():

    def test_creation_and_cancel(self, asys):
        sta = asys.createActor(StringBatchTestActor)
        sub = Subscription(sta)
        cancel = Cancel(None, sub, sub)
        asys.tell(sub, cancel)

    def test_push(self, asys):
        pass

    def test_request(self, asys):
        pass

    def test_on_next(self, asys):
        pass
    
    def test_cancel(self, asys):
        pass

    def test_subscription(self, asys):
        pass

    def test_complete(self):
        pass
