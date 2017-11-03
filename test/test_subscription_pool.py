'''
Test subscription pool.

Created on Nov 2, 2017

@author: aevans
'''


import pytest
from thespian.actors import ActorSystem


@pytest.fixture(scope="module")
def asys():
    return ActorSystem()


class TestSubscriptionPool():

    def __init__(self):
        pass
