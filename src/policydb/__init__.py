from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from policydb.api import PolicyDB

__all__ = ["PolicyDB"]
__version__ = "0.1.0"


def __getattr__(name: str):
    if name == "PolicyDB":
        from policydb.api import PolicyDB

        return PolicyDB
    raise AttributeError(name)
