'''
Test a Publisher.

Created on Nov 2, 2017

@author: aevans
'''


import pytest
from thespian.actors import ActorSystem

from reactive.streams.base_objects.publisher import Publisher


@pytest.fixture(scope="module")
def asys():
    return ActorSystem()


class TestPublisher():

    def test_creation(self, asys):
        pub = Publisher()

    def test_subscription(self, asys):
        pass

    def test_desubscription(self, asys):
        pass
