# -*- coding: utf-8 -*-

import pytest

from boto.dynamodb2.types import *
from kiwi import *


class TestQuery(object):
    def test_basic(self, UserAction):
        assert 3 == UserAction.query().filter(UserAction.id == 1).count()

    def test_construct(self, UserAction):
        query = UserAction.query()
        assert query._mapper is UserAction.__mapper__
        assert query._index is None
        assert query._attributes is None
        assert query._consistent is False
        assert query._max_page_size is None
        assert query._reverse is False
        assert query._limit is None

        query = UserAction.query(consistent=True, limit=5, max_page_size=3)
        assert query._max_page_size == 3
        assert query._consistent is True
        assert query._limit == 5

    def test_check_index(self, UserAction):
        query = UserAction.query(index=UserAction.dur_index)
        assert query._index == UserAction.dur_index.name

        query = UserAction.query(index=UserAction.dur_index.name)
        assert query._index == UserAction.dur_index.name

        query = UserAction.query(index=UserAction.result_index)
        assert query._index == UserAction.result_index.name

        with pytest.raises(ArgumentError):
            UserAction.query(index='abc')

        with pytest.raises(ArgumentError):
            UserAction.query(index=Index())

    def test_check_attributes(self, UserAction):
        query = UserAction.query(attributes=[UserAction.time])
        assert query._attributes == ['time']

    def test_filters(self, UserAction):
        q, f = UserAction.query().filter(
                    UserAction.id == 3, UserAction.time < 8)._build_filters()
        assert q == {}
        assert f == {'id__eq': 3, 'time__lt': 8}

        q, f = UserAction.query(index=UserAction.dur_index).filter(
                    UserAction.id == 3, UserAction.time < 8)._build_filters()
        assert q == {'id__eq': 3}
        assert f == {'time__lt': 8}

        q, f = UserAction.query(index=UserAction.result_index).filter(
                    UserAction.id == 3, UserAction.time < 8)._build_filters()
        assert q == {'time__lt': 8}
        assert f == {'id__eq': 3}

        query = UserAction.query().filter(UserAction.duration > 3)
        q, f = query._build_filters()
        assert q == {'duration__gt': 3}
        assert f == {}
        with pytest.raises(InvalidRequestError):
            query.all()

        with pytest.raises(ArgumentError):
            UserAction.query().filter(3, 2)

    def test_reverse(self, UserAction):
        query = UserAction.query()
        assert query._reverse is False

        query.desc()
        assert query._reverse is True

        query.desc()
        assert query._reverse is True

        query.asc()
        assert query._reverse is False

        query.asc()
        assert query._reverse is False

        query.desc()
        assert query._reverse is True

    def test_limit(self, UserAction):
        query = UserAction.query()
        assert query._limit is None

        query.limit(3)
        assert query._limit == 3

        query.limit(5)
        assert query._limit == 5

        query = UserAction.query().filter(UserAction.id == 2)
        assert query.count() == 4

        query = query.clone().limit(2)
        assert query.count() == 2

        query = query.clone().limit(5)
        assert query.count() == 4

        query = query.clone().limit(1)
        assert query.count() == 1

    def test_first(self, UserAction):
        query = UserAction.query().filter(UserAction.id == 2)
        ua = query.first()
        assert ua.time == 2

        query = query.clone().desc()
        ua = query.first()
        assert ua.time == 10
