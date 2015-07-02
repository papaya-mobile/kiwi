# -*- coding: utf-8 -*-

import pytest
import time

from boto.dynamodb2.types import NUMBER

from kiwi import MetaData, Table
from kiwi import Field, HashKeyField, RangeKeyField
from kiwi import LocalAllIndex, GlobalAllIndex


@pytest.fixture(scope="session")
def local_db():
    from boto.dynamodb2.layer1 import DynamoDBConnection
    conn = DynamoDBConnection(host="localhost", port=8000,
                              aws_access_key_id="kiwi",
                              aws_secret_access_key="kiwi",
                              is_secure=False)
    return conn


@pytest.fixture(scope="class")
def metadata(request, local_db):
    md = MetaData(connection=local_db)

    def teardown():
        md.drop_all()
        md.clear()

    request.addfinalizer(teardown)
    return md


@pytest.fixture(scope="class")
def User(metadata):
    class User(Table):
        __metadata__ = metadata

        id = HashKeyField(data_type=NUMBER)
        name = Field()
        birth = Field(data_type=NUMBER, default=lambda: time.time())

    User.__mapper__.create_table()

    User(id=1, name='a').save()
    User(id=2, name='b').save()
    User(id=3, name='c').save()
    User(id=4, name='d').save()
    User(id=5, name='e').save()
    User(id=6, name='f').save()
    User(id=7, name='g').save()
    User(id=8, name='h').save()
    User(id=9, name='i').save()

    return User


@pytest.fixture(scope="class")
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

    UserAction.__mapper__.create_table()

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
