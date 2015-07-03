# -*- coding: utf-8 -*-

import pytest

from kiwi.field import *
from kiwi.exceptions import *
from kiwi import dynamo


class TestField(object):
    def test_basic(self):
        f = Field('f')
        assert isinstance(f, Field)
        assert f.name == 'f'
        assert f.key == 'f'
        assert f.data_type == dynamo.STRING
        assert f.default() is None
        assert f.attr_type is None

        kf = HashKeyField('f')
        assert kf.attr_type is dynamo.HashKey

        rf = RangeKeyField('f')
        assert rf.attr_type is dynamo.RangeKey

    def test_descriptor(self):
        class Owner(object):
            f = Field('f')
            def __init__(self):
                self._item = {}
        Owner.f.owner = Owner

        owner = Owner()
        assert owner.f is None
        owner.f = 3
        assert owner.f == 3
        owner.f = 5
        assert owner.f == 5
        assert owner._item['f'] == 5

        with pytest.raises(InvalidRequestError):
            del owner.f

    def test_expression(self):
        f = Field('f')

        assert (f == 3).schema() == ('f__eq', 3)
        assert (f < 3).schema() == ('f__lt', 3)
        assert (f <= 3).schema() == ('f__lte', 3)
        assert (f >= 3).schema() == ('f__gte', 3)
        assert (f > 3).schema() == ('f__gt', 3)
        assert (f.between_(2, 4)).schema() == ('f__between', (2, 4))
        assert (f.beginswith_('a')).schema() == ('f__beginswith', 'a')

        f = KeyField('f')
        assert (f != 3).schema() == ('f__ne', 3)
        assert (f.in_(3)).schema() == ('f__in', 3)
        assert (f.notnone_()).schema() == ('f__nnull', None)
        assert (f.isnone_()).schema() == ('f__null', None)
        assert (f.contains_(3)).schema() == ('f__contains', 3)
        assert (f.notcontains_(3)).schema() == ('f__ncontains', 3)


class TestLocalAllIndex(object):
    def test_basic(self):
        parts = [Field('a'), Field('b')]
        idx = LocalAllIndex(name='a', parts=parts)
        assert idx.idx_type == dynamo.AllIndex
        assert idx.name == 'a'
        assert idx.parts == parts

    def test_map(self):
        parts = [Field('a'), Field('b')]
        idx = LocalAllIndex(name='a', parts=parts)

        didx = idx.map()

        assert isinstance(didx, dynamo.AllIndex)
        assert didx.name == 'a'
        hk, rk = didx.parts

        assert isinstance(hk, dynamo.HashKey)
        assert hk.name == 'a'
        assert hk.data_type == dynamo.STRING
        assert isinstance(rk, dynamo.RangeKey)
        assert rk.name == 'b'
        assert rk.data_type == dynamo.STRING


class TestLocalKeysOnlyIndex(object):
    def test_basic(self):
        parts = [Field('a'), Field('b')]
        idx = LocalKeysOnlyIndex(name='a', parts=parts)
        assert idx.idx_type == dynamo.KeysOnlyIndex
        assert idx.name == 'a'
        assert idx.parts == parts

    def test_map(self):
        parts = [Field('a'), Field('b')]
        idx = LocalKeysOnlyIndex(name='a', parts=parts)

        didx = idx.map()

        assert isinstance(didx, dynamo.KeysOnlyIndex)
        assert didx.name == 'a'
        hk, rk = didx.parts

        assert isinstance(hk, dynamo.HashKey)
        assert hk.name == 'a'
        assert hk.data_type == dynamo.STRING
        assert isinstance(rk, dynamo.RangeKey)
        assert rk.name == 'b'
        assert rk.data_type == dynamo.STRING


class TestLocalIncludeIndex(object):
    def test_basic(self):
        parts = ['a', 'b']
        includes = ['c', 'd']
        idx = LocalIncludeIndex(name='a', parts=parts, includes=includes)
        assert idx.idx_type == dynamo.IncludeIndex
        assert idx.name == 'a'
        assert idx.parts == parts
        assert idx.includes == includes

    def test_map(self):
        parts = [Field('a'), Field('b')]
        includes = [Field('c'), Field('d')]
        idx = LocalIncludeIndex(name='a', parts=parts, includes=includes)

        didx = idx.map()

        assert isinstance(didx, dynamo.IncludeIndex)
        assert didx.name == 'a'
        assert didx.includes_fields == ['c', 'd']


class TestGlobalAllIndex(object):
    def test_basic(self):
        parts = ['a', 'b']
        throughput = {'read': 1, 'write': 2}
        idx = GlobalAllIndex(name='a', parts=parts, throughput=throughput)
        assert idx.idx_type == dynamo.GlobalAllIndex
        assert idx.name == 'a'
        assert idx.parts == parts
        assert idx.throughput == throughput

    def test_map(self):
        parts = [Field('a'), Field('b')]
        throughput = {'read': 1, 'write': 2}
        idx = GlobalAllIndex(name='a', parts=parts, throughput=throughput)

        didx = idx.map()

        assert isinstance(didx, dynamo.GlobalAllIndex)
        assert didx.name == 'a'
        assert didx.throughput == throughput


class TestGlobalKeysOnlyIndex(object):
    def test_basic(self):
        parts = ['a', 'b']
        throughput = {'read': 1, 'write': 2}
        idx = GlobalKeysOnlyIndex(name='a', parts=parts, throughput=throughput)
        assert idx.idx_type == dynamo.GlobalKeysOnlyIndex
        assert idx.name == 'a'
        assert idx.parts == parts
        assert idx.throughput == throughput

    def test_map(self):
        parts = [Field('a'), Field('b')]
        throughput = {'read': 1, 'write': 2}
        idx = GlobalKeysOnlyIndex(name='a', parts=parts, throughput=throughput)

        didx = idx.map()

        assert isinstance(didx, dynamo.GlobalKeysOnlyIndex)
        assert didx.name == 'a'
        assert didx.throughput == throughput


class TestGlobalIncludeIndex(object):
    def test_basic(self):
        parts = ['a', 'b']
        includes = [Field('c'), Field('d')]
        throughput = {'read': 1, 'write': 2}
        idx = GlobalIncludeIndex(name='a', parts=parts,
                                 includes=includes,
                                 throughput=throughput)

        assert idx.idx_type == dynamo.GlobalIncludeIndex
        assert idx.name == 'a'
        assert idx.parts == parts
        assert idx.includes == includes
        assert idx.throughput == throughput

    def test_map(self):
        parts = [Field('a'), Field('b')]
        includes = [Field('c'), Field('d')]
        throughput = {'read': 1, 'write': 2}
        idx = GlobalIncludeIndex(name='a', parts=parts,
                                 includes=includes,
                                 throughput=throughput)

        didx = idx.map()

        assert isinstance(didx, dynamo.GlobalIncludeIndex)
        assert didx.name == 'a'
        assert didx.includes_fields == ['c', 'd']
        assert didx.throughput == throughput
