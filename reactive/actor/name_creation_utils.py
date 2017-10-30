'''
Created on Oct 29, 2017

@author: aevans
'''

from random import Random
import sys
from atomos.atomic import AtomicLong


num = AtomicLong(0)


def get_name():
    """
    Get a randomized name with the actor number.

    :return: The base name
    :rtype: str()
    """
    base = str(num.get_and_add(1))
    base += "_"
    base += str(Random().randint(0, sys.maxsize))
    return base
