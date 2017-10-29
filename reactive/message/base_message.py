'''
Created on Oct 29, 2017

@author: aevans
'''


class Message():

    def __init__(self, payload, target, sender):
        self.payload = payload
        self.sender = sender
        self.target = target

    def __repr__(self, *args, **kwargs):
        rep = "{}(payload={}, target={}, sender={})"
        rep = rep.format(str(type(self)), self.payload, self.target, self.sender)
        return rep

    def __str__(self, *args, **kwargs):
        return self.__repr__()
