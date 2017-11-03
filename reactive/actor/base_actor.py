'''
Created on Oct 29, 2017

@author: aevans
'''
from thespian.actors import Actor

from reactive.actor.state import ActorState


class BaseActor(Actor):
    
    def __init__(self):
        """
        Constructor
        """
        super().__init__()
        self.state = ActorState.CREATED
