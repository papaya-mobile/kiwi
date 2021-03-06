# -*- coding: utf-8 -*-

from builtins import object
from future.utils import with_metaclass

import pytest
import time

from boto.dynamodb2.types import *
from boto.dynamodb2.fields import HashKey, RangeKey

import kiwi
from kiwi import *
from kiwi import dynamo


@pytest.fixture(autouse=True)
def clear_metadatas(request):
    def clear():
        for md in kiwi.metadatas:
            md.clear()
    request.addfinalizer(clear)


def test_basic():
    class User(Table):
        __tablename__ = 'user'

        id = HashKeyField()
        name = Field(data_type=NUMBER)

    assert hasattr(User, '__mapper__')

    mapper = User.__mapper__
    assert mapper.class_ == User


class TestMetaData(object):
    def test_default(self):
        class User(Table):
            id = HashKeyField()
        mapper = User.__mapper__

        assert mapper.metadata == metadata
        assert mapper in metadata

    def test_assign(self):
        md = MetaData()

        class User(Table):
            id = HashKeyField()
            __metadata__ = md
        mapper = User.__mapper__

        assert mapper.metadata == md
        assert mapper in md

    def test_inherit_1(self):
        md = MetaData()

        class Hi(object):
            __metadata__ = md

        class User(Table, Hi):
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.metadata == md
        assert mapper in md

    def test_inherit_2(self):
        md = MetaData()

        class Hi(Table):
            __metadata__ = md
            id = HashKeyField()

        class User(Hi):
            name = Field()

        for cls in [Hi, User]:
            mapper = cls.__mapper__
            assert mapper.metadata == md
            assert mapper in md

    def test_table(self):
        class DummyDynamizer(Dynamizer):
            pass

        md = MetaData(connection='DummyConnection',
                      dynamizer=DummyDynamizer)

        class User(Table):
            id = HashKeyField()
            __metadata__ = md
        mapper = User.__mapper__

        assert isinstance(mapper.table, dynamo.Table)


class TestTablename(object):
    def test_basic(self):
        class User(Table):
            __tablename__ = 'iamname'
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.tablename == 'iamname'

    def test_default(self):
        class User(Table):
            id = HashKeyField()
        assert User.__mapper__.tablename == 'user'

        class UserName(Table):
            id = HashKeyField()
        assert UserName.__mapper__.tablename == 'user_name'

    def test_inherit(self):
        class Hi(object):
            __tablename__ = 'iamname'
            name = Field()

        class User(Table, Hi):
            id = HashKeyField()

        class BoomUser(User):
            __tablename__ = "boom"
            boom = Field()

        assert User.__mapper__.tablename == 'iamname'
        assert BoomUser.__mapper__.tablename == 'boom'


class TestThroughput(object):
    def test_default(self):
        class User(Table):
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.throughput is None

    def test_basic(self):
        tp = {'read': 1, 'write': 5}

        class User(Table):
            __throughput__ = tp
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.throughput == tp

    def test_inherit_1(self):
        tp = {'read': 1, 'write': 6}
        Table.__throughput__ = tp

        class User(Table):
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.throughput == tp

        del Table.__throughput__

    def test_inherit_2(self):
        tp = {'read': 1, 'write': 6}

        class Hi(object):
            __throughput__ = tp

        class User(Table, Hi):
            id = HashKeyField()

        mapper = User.__mapper__
        assert mapper.throughput == tp


class TestSchema(object):
    def test_not_schema(self):
        with pytest.raises(NoPrimaryKeyError):
            class User(Table):
                pass

    def test_hashkey(self):
        class User(Table):
            id = HashKeyField()

        mapper = User.__mapper__
        assert 1 == len(mapper.schema)
        hk = mapper.schema[0]
        assert isinstance(hk, HashKey)
        assert hk.name == 'id'
        assert hk.data_type == STRING

    def test_rangekey(self):
        class User(Table):
            id = HashKeyField()
            name = RangeKeyField(data_type=NUMBER)

        self._check_rangekey(User)

    def _check_rangekey(self, User):
        mapper = User.__mapper__
        assert 2 == len(mapper.schema)

        hk = mapper.schema[0]
        assert isinstance(hk, HashKey)
        assert hk.name == 'id'
        assert hk.data_type == STRING

        rk = mapper.schema[1]
        assert isinstance(rk, RangeKey)
        assert rk.name == 'name'
        assert rk.data_type == NUMBER

    def test_inherit_1(self):
        class Hi(object):
            id = HashKeyField()

        class User(Table, Hi):
            name = RangeKeyField(data_type=NUMBER)

        self._check_rangekey(User)

    def test_inherit_2(self):
        class Hi(TableBase):
            id = HashKeyField()

        class Table(with_metaclass(TableMeta, Hi)):
            pass

        class User(Table, Hi):
            name = RangeKeyField(data_type=NUMBER)

        self._check_rangekey(User)

    def test_inherit_3(self):
        class Hi(Table):
            id = HashKeyField()

        class User(Hi):
            name = RangeKeyField(data_type=NUMBER)

        self._check_rangekey(User)


class TestAttribute(object):
    def test_basic(self):
        T = 42

        class User(Table):
            id = HashKeyField()
            name = Field()
            birth = Field(data_type=NUMBER, default=T);

        mapper = User.__mapper__
        assert len(mapper.attributes) == 3
        assert User.id is mapper.attributes['id']
        assert User.name is mapper.attributes['name']
        assert User.birth is mapper.attributes['birth']

        u = User(id='3', name='name')
        assert u.id == '3'
        assert u.name == 'name'
        assert u.birth == T

    def test_inherit_1(self):
        class Hi(object):
            id = HashKeyField()

        class User(Table, Hi):
            name = Field(data_type=NUMBER)

        mapper = User.__mapper__
        assert User.id is mapper.attributes['id']
        assert User.name is mapper.attributes['name']

    def test_inherit_2(self):
        class Hi(Table):
            id = HashKeyField()

        class User(Hi):
            name = Field(data_type=NUMBER)

        mapper = User.__mapper__
        assert User.id is mapper.attributes['id']
        assert User.name is mapper.attributes['name']


class TestIndex(object):
    def test_default(self):
        class User(Table):
            id = HashKeyField()
        mapper = User.__mapper__
        assert mapper.indexes == {}
        assert mapper.global_indexes == {}

    def test_basic(self):
        class User(Table):
            id = HashKeyField()
            name = RangeKeyField(data_type=NUMBER)
            birth = Field(data_type=NUMBER, default=lambda: time.time())

            i1 = GlobalAllIndex(parts=[id, birth])
