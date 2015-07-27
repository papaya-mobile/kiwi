# -*- coding: utf-8 -*-

from builtins import object

import pytest

from boto.dynamodb2.types import *
from kiwi import *


class TestQuery(object):
    def test_basic(self, UserAction):
        assert 3 == UserAction.query().onkeys(UserAction.id == 1).count()

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

    def test_keyconds_primary(self, UserAction):
        # query on primary key
        query = UserAction.query()
        f = query._build_raw_filters(query._key_conds)
        assert f == {}

        with pytest.raises(ArgumentError):
            query.onkeys(4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time == 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id < 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, UserAction.name > 3)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, UserAction.time.isnone_())

        query = UserAction.query().onkeys(UserAction.id == 3)
        f = query._build_raw_filters(query._key_conds)
        assert f == {'id__eq': 3}

        with pytest.raises(InvalidRequestError):
            query.onkeys(UserAction.id == 4)

        query = UserAction.query().onkeys(
            UserAction.id == 3, UserAction.time < 8)
        f = query._build_raw_filters(query._key_conds)
        assert f == {'id__eq': 3, 'time__lt': 8}

    def test_keyconds_local_idx(self, UserAction):
        # query on local index
        query = UserAction.query(index=UserAction.result_index)
        f = query._build_raw_filters(query._key_conds)
        assert f == {}

        with pytest.raises(ArgumentError):
            query.onkeys(4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time == 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id < 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, UserAction.name > 3)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 3, UserAction.result.isnone_())

        query = UserAction.query(
            index=UserAction.result_index).onkeys(UserAction.id == 3)
        f = query._build_raw_filters(query._key_conds)
        assert f == {'id__eq': 3}

        with pytest.raises(InvalidRequestError):
            query.onkeys(UserAction.id == 4)

        query = UserAction.query(index=UserAction.result_index).onkeys(
            UserAction.id == 3, UserAction.result == 'a')
        f = query._build_raw_filters(query._key_conds)
        assert f == {'id__eq': 3, 'result__eq': 'a'}

    def test_keyconds_global_idx(self, UserAction):
        # query on global index
        query = UserAction.query(index=UserAction.dur_index)
        f = query._build_raw_filters(query._key_conds)
        assert f == {}

        with pytest.raises(ArgumentError):
            query.onkeys(4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.id == 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time < 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time == 3, 4)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time == 3, UserAction.name > 3)
        with pytest.raises(ArgumentError):
            query.onkeys(UserAction.time == 3, UserAction.duration.isnone_())

        query = UserAction.query(
            index=UserAction.dur_index).onkeys(UserAction.time == 8)
        f = query._build_raw_filters(query._key_conds)
        assert f == {'time__eq': 8}

        with pytest.raises(InvalidRequestError):
            query.onkeys(UserAction.time == 4)

        query = UserAction.query(index=UserAction.dur_index).onkeys(
            UserAction.time == 8, UserAction.duration > 10)
        f = query._build_raw_filters(query._key_conds)
        assert f == {'time__eq': 8, 'duration__gt': 10}

    def test_filters(self, UserAction):
        query = UserAction.query().filter(UserAction.id == 3)
        q = query._build_raw_filters(query._filters)
        assert q == {'id__eq': 3}

        query = UserAction.query().filter(UserAction.duration > 3)
        q = query._build_raw_filters(query._filters)
        assert q == {'duration__gt': 3}

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

        query = UserAction.query().onkeys(UserAction.id == 2)
        assert query.count() == 4

        query = query.clone().limit(2)
        assert query.count() == 2

        query = query.clone().limit(5)
        assert query.count() == 4

        query = query.clone().limit(1)
        assert query.count() == 1

    def test_first(self, UserAction):
        query = UserAction.query().onkeys(UserAction.id == 2)
        ua = query.first()
        assert ua.time == 2

        query = query.clone().desc()
        ua = query.first()
        assert ua.time == 10
