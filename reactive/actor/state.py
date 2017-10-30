'''
Created on Oct 29, 2017

@author: aevans
'''

from enum import Enum


class ActorState(Enum):
    CREATED = 1
    LIMBO = 2
    TERMINATED = 3