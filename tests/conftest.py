from pathlib import Path

import pytest

from policydb import PolicyDB


@pytest.fixture(scope="session")
def root():
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def db(root):
    return PolicyDB.open(root)
