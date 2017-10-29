'''
Created on Oct 29, 2017

@author: aevans
'''

from random import Random
import sys
from atomos.atomic import AtomicLong


class NameCreator():

    def __init__(self):
        self.__num = AtomicLong(0)

    @staticmethod
    def get_name(self):
        """
        Get a randomized name with the actor number.

        :return: The base name
        :rtype: str()
        """
        base = str(self.__num.get_and_add(1))
        base += "_"
        base += str(Random().randint(0, sys.maxsize))
        return base
