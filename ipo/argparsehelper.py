"""
Mainly type hinting helper for argparse.
"""

from typing import Protocol

"""
Protocol for argparse._SubParsersAction.

As _SubParsersAction is an internal implementation detail, make sure we
just rely on the methods it exposes (i.e. duck typing).
"""
class AddParser(Protocol):
    def add_parser(self, *args, **kvargs):
        ...
