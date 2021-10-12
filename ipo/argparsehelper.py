"""
Mainly type hinting helper for argparse.
"""
from argparse import ArgumentParser
from typing import Protocol


class AddParser(Protocol):
    """
    Protocol for argparse._SubParsersAction.

    As _SubParsersAction is an internal implementation detail, make sure we
    just rely on the methods it exposes (i.e. duck typing).
    """
    def add_parser(self, *args, **kvargs) -> ArgumentParser:
        """ As per argparse, returns a new ArgumentParser """
        ...
