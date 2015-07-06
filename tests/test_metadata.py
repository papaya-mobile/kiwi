# -*- coding: utf-8 -*-

from builtins import object

import pytest

from kiwi.metadata import MetaData
from kiwi.exceptions import *


class DummyMapper(object):
    def __init__(self, tname):
        self.tablename = tname
        self.throughput = None


def test_basic():
    md = MetaData()
    assert md._tables == {}
    assert md.connection is None
    assert md.throughput is None

    md.configure(connection=3, throughput=4)
    assert md.connection == 3
    assert md.throughput == 4

    f = lambda cls: cls.__name__
    md.configure(tablename_factory=f)
    assert md.tablename_factory is f

    md.configure()
    assert md.connection == 3
    assert md.throughput == 4
    assert md.tablename_factory is f

    a = DummyMapper('a')
    md.add(a)

    with pytest.raises(InvalidRequestError):
        md.configure()


def test_tables():
    md = MetaData()
    assert md._tables == {}

    md.add(DummyMapper('a'))
    md.add(DummyMapper('b'))
    md.add(DummyMapper('c'))
    md.add(DummyMapper('d'))

    with pytest.raises(InvalidRequestError):
        md.add(DummyMapper('a'))

    assert DummyMapper('a') in md
    md.remove(DummyMapper('a'))
    assert DummyMapper('a') not in md

    assert set(md) == set(['b', 'c', 'd'])
    assert len(list(md.values())) == 3
    assert len(list(md.items())) == 3

    md.clear()
    assert len(list(md)) == 0
