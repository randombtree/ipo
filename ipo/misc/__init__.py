"""
Miscellaneous items.
"""
from typing import TypeVar, Optional

DefaultType = TypeVar('DefaultType')


def default(optional: Optional[DefaultType], default_value: DefaultType) -> DefaultType:
    """ Return the default type if the optional value is None """
    return optional if optional is not None else default_value
