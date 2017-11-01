'''
Stream related errors.

Created on Oct 31, 2017

@author: aevans
'''


class SourceError(Exception):

    def __init__(self, message):
        super(SourceError, self).__init__(message)


class ProcessNodeError(Exception):

    def __init__(self, message):
        super(ProcessNodeError, self).__init__(message)


class RouterNodeError(Exception):
    
    def __init__(self, message):
        super(RouterNodeError, self).__init__(message)


class SinkError(Exception):

    def __init__(self, message):
        super(SinkError, self).__init__(message)
