# -*- coding: utf-8 -*-

import pytest
import time

from boto.dynamodb2.types import *
from boto.dynamodb2.fields import HashKey, RangeKey

from kiwi import *


def test_basic():
    class User(Table):
        __tablename__ = 'user'

        id = Field(attr_type=FieldType.HASH)
        name = Field(data_type=NUMBER)

    assert hasattr(User, '__mapper__')

    mapper = User.__mapper__
    assert mapper.class_  == User

class TestMetaData(object):
    def test_default(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
        mapper = User.__mapper__

        assert mapper.metadata == metadata
        assert mapper in metadata

    def test_assign(self):
        md = MetaData()
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
            __metadata__ = md
        mapper = User.__mapper__

        assert mapper.metadata == md
        assert mapper in md

    def test_inherit_1(self):
        md = MetaData()
        Table.__metadata__ = md

        class User(Table):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.metadata == md
        assert mapper in md

        del Table.__metadata__

    def test_inherit_2(self):
        md = MetaData()

        class Hi(object):
            __metadata__ = md
        class User(Table, Hi):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.metadata == md
        assert mapper in md


class TestTablename(object):
    def test_basic(self):
        class User(Table):
            __tablename__ = 'iamname'
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.tablename == 'iamname'

    def test_default(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
        assert User.__mapper__.tablename == 'user'

        class UserName(Table):
            id = Field(attr_type=FieldType.HASH)
        assert UserName.__mapper__.tablename == 'user_name'

    def test_inherit(self):
        class Hi(object):
            __tablename__ = 'iamname'

        class User(Table, Hi):
            id = Field(attr_type=FieldType.HASH)

        assert User.__mapper__.tablename == 'iamname'

class TestThroughput(object):
    def test_default(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.throughput is None

    def test_basic(self):
        tp = { 'read': 1, 'write': 5}
        class User(Table):
            __throughput__ = tp
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.throughput == tp

    def test_inherit_1(self):
        tp = { 'read': 1, 'write': 6}
        Table.__throughput__ = tp

        class User(Table):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.throughput == tp

        del Table.__throughput__

    def test_inherit_2(self):
        tp = { 'read': 1, 'write': 6}
        class Hi(object):
            __throughput__ = tp
        class User(Table, Hi):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert mapper.throughput == tp

class TestSchema(object):
    def test_not_schema(self):
        with pytest.raises(Exception):
            class User(Table):
                pass

    def test_hashkey(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)

        mapper = User.__mapper__
        assert 1 == len(mapper.schema)
        hk = mapper.schema[0]
        assert isinstance(hk, HashKey)
        assert hk.name == 'id'
        assert hk.data_type == STRING

    def test_rangekey(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
            name = Field(attr_type=FieldType.RANGE, data_type=NUMBER)

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
            id = Field(attr_type=FieldType.HASH)

        class User(Table, Hi):
            name = Field(attr_type=FieldType.RANGE, data_type=NUMBER)

        self._check_rangekey(User)

    def test_inherit_2(self):
        class Hi(TableBase):
            id = Field(attr_type=FieldType.HASH)

        class Table(Hi):
            __metaclass__ = TableMeta

        class User(Table, Hi):
            name = Field(attr_type=FieldType.RANGE, data_type=NUMBER)

        self._check_rangekey(User)

class TestAttribute(object):
    def test_basic(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
            name = Field()
            birth = Field(data_type=NUMBER, default=lambda: time.time())

        mapper = User.__mapper__
        assert len(mapper.attributes) == 3
        assert User.id is mapper.attributes['id']
        assert User.name is mapper.attributes['name']
        assert User.birth is mapper.attributes['birth']


class TestIndex(object):
    def test_default(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
        mapper = User.__mapper__
        assert mapper.indexes is None
        assert mapper.global_indexes is None

    def test_basic(self):
        class User(Table):
            id = Field(attr_type=FieldType.HASH)
            name = Field(attr_type=FieldType.RANGE, data_type=NUMBER)
