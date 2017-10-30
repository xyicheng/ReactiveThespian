'''
Created on Oct 29, 2017

@author: aevans
'''

import pytest
from thespian.actors import ActorSystem
from reactive.routers.pub_sub import PubSub
from reactive.actor.routee import ActorRoutee
from reactive.message.router_messages import Subscribe, GetNumRoutees,\
    DeSubscribe, Broadcast
from reactive.message.base_message import Message


class RouteeAdd(Message):
    pass


class PubTestRoutee(ActorRoutee):

    def __init__(self):
        super().__init__()

    def receiveMessage(self, msg, sender):
        super().receiveMessage(msg, sender)
        if isinstance(RouteeAdd):
            payload = msg.payload
            payload += 1
            self.send(sender, payload)


@pytest.fixture(scope="module")
def asys():
    asys = ActorSystem("multiprocQueueBase")
    yield asys
    asys.shutdown()

class TestPubSub():

    def test_creation(self, asys):
        asys.createActor(PubSub)

    def test_subscribe_routee(self, asys):
        psub = asys.createActor(PubSub)
        routee = asys.createActor(PubTestRoutee)
        sub = Subscribe(routee, psub, None)
        asys.tell(psub, sub)
        count_msg = GetNumRoutees(None, psub, None)
        ct = asys.ask(psub, count_msg)
        assert ct == 1

    def test_desubscribe(self, asys):
        psub = asys.createActor(PubSub)
        routee = asys.createActor(PubTestRoutee)
        routeeb = asys.createActor(PubTestRoutee)
        sub = Subscribe(routee, psub, None)
        asys.tell(psub, sub)
        subb = Subscribe(routeeb, psub, None)
        asys.tell(psub, subb)
        count_msg = GetNumRoutees(None, psub, None)
        desub = DeSubscribe(routee, psub, None)
        asys.tell(psub, desub)
        ct = asys.ask(psub, count_msg)
        assert ct == 1

    def test_broadcast(self, asys):
        psub = asys.createActor(PubSub)
        routee = asys.createActor(PubTestRoutee)
        routeeb = asys.createActor(PubTestRoutee)
        sub = Subscribe(routee, psub, None)
        asys.tell(psub, sub)
        subb = Subscribe(routeeb, psub, None)
        asys.tell(psub, subb)
        bmsg = Broadcast("Hello", psub, None)
        asys.tell(psub, bmsg)

    def test_ask_routee(self, asys):
        psub = asys.createActor(PubSub)
        routee = asys.createActor(PubTestRoutee)
        routeeb = asys.createActor(PubTestRoutee)
        sub = Subscribe(routee, psub, None)
        asys.tell(psub, sub)
        subb = Subscribe(routeeb, psub, None)
        asys.tell(psub, subb)
        radd = RouteeAdd(1, psub, None)
        res_val = asys.ask(psub, radd)
        assert res_val == 2
