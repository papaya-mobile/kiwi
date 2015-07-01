# -*- coding: utf-8 -*-

import pytest

from boto.dynamodb2.types import *
from kiwi import *

@pytest.fixture
def UserAction(metadata):
    class UserAction(Table):
        __metadata__ = metadata
        id = HashKeyField(data_type=NUMBER)
        time = RangeKeyField(data_type=NUMBER)
        name = Field()
        duration = Field(data_type=NUMBER)
        result = Field()

        dur_index = GlobalAllIndex(parts=[time, duration])
        result_index = LocalAllIndex(parts=[id, result])

    metadata.create_all()

    UserAction(id=1, time=1, name="hello", duration=2, result="ok").save()
    UserAction(id=2, time=2, name="hillo", duration=5, result="ko").save()
    UserAction(id=2, time=3, name="hello", duration=1, result="ok").save()
    UserAction(id=1, time=4, name="hillo", duration=4, result="fail").save()
    UserAction(id=3, time=5, name="23ewsx", duration=2, result="ok").save()
    UserAction(id=5, time=6, name="okljhye", duration=2, result="ok").save()
    UserAction(id=3, time=7, name="qsgs", duration=8, result="hi").save()
    UserAction(id=1, time=8, name="efgh", duration=2, result="ok").save()
    UserAction(id=2, time=9, name="abcd", duration=1, result="yes").save()
    UserAction(id=2, time=10, name="ki", duration=2, result="nono").save()
    UserAction(id=5, time=11, name="48jrj", duration=3, result="enen").save()
    UserAction(id=4, time=12, name="ello", duration=2, result="ok").save()
    UserAction(id=4, time=13, name="h-4-13", duration=2, result="bye").save()

    return UserAction

def test_basic(UserAction):
    assert 3 == UserAction.query().filter(UserAction.id == 1).count()

def test_construct(UserAction):
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

def test_check_index(UserAction):
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

def test_check_attributes(UserAction):
    query = UserAction.query(attributes=[UserAction.time])
    assert query._attributes == ['time']

def test_filters(UserAction):
    q, f = UserAction.query().filter(
            UserAction.id == 3, UserAction.time < 8)._build_filters()
    assert q == {}
    assert f == { 'id__eq' : 3, 'time__lt' : 8}

    q, f = UserAction.query(index=UserAction.dur_index).filter(
            UserAction.id == 3, UserAction.time < 8)._build_filters()
    assert q == { 'id__eq' : 3 }
    assert f == {'time__lt' : 8}

    q, f = UserAction.query(index=UserAction.result_index).filter(
            UserAction.id == 3, UserAction.time < 8)._build_filters()
    assert q == {'time__lt' : 8}
    assert f == { 'id__eq' : 3 }

    query = UserAction.query().filter(UserAction.duration > 3)
    q, f = query._build_filters()
    assert q == {'duration__gt' : 3}
    assert f == {}
    with pytest.raises(InvalidRequestError):
        query.all()

    with pytest.raises(ArgumentError):
        UserAction.query().filter(3, 2)


def test_reverse(UserAction):
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

def test_limit(UserAction):
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

def test_first(UserAction):
    query = UserAction.query().filter(UserAction.id == 2)
    ua = query.first()
    assert ua.time == 2

    query = query.clone().desc()
    ua = query.first()
    assert ua.time == 10



