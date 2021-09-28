""" ICOND common events """
from . message import IconMessage

class ShutdownEvent:
    """ Event signalling a shutdown """
    ...


class MessageEvent:
    """ Message delivery event """
    msg: IconMessage

    def __init__(self, msg: IconMessage):
        """ msg: The icon message """
        self.msg = msg

    def get(self):
        """ Get the message """
        return self.msg
