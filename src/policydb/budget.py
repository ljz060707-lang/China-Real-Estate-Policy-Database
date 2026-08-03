"""Shared budget-stop exceptions used by network-aware orchestration.

The exceptions live in a dependency-light module so the crawl layer can stop
before an outbound request without importing the full-sync controller.
"""

from __future__ import annotations


class BudgetExceeded(RuntimeError):
    """A configured API or network budget cannot admit another request."""


class HttpBudgetExceeded(BudgetExceeded):
    """Raised before a real HTTP attempt when the HTTP ledger is exhausted."""
