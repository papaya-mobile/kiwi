# -*- coding: utf-8 -*-

from builtins import zip
from builtins import object

__all__ = ['Field', 'KeyField', 'HashKeyField', 'RangeKeyField',
           'Index', 'LocalIndex', 'GlobalIndex', 'IncludeIndex',
           'LocalAllIndex', 'LocalKeysOnlyIndex', 'LocalIncludeIndex',
           'GlobalAllIndex', 'GlobalKeysOnlyIndex', 'GlobalIncludeIndex',
           ]


from . import dynamo
from .expression import Filterable
from .exceptions import *


class SchemaBase(object):
    def __init__(self, *args, **kwargs):
        self.owner = None
        self._configured = False

    def configure(self, class_, name=None):
        self.owner = class_
        if name and not self.name:
            self.key = self.name = name
        self._configured = True


class Field(SchemaBase, Filterable):
    attr_type = None

    def __init__(self, name=None, data_type=dynamo.STRING, default=None):
        super(Field, self).__init__()
        self.key = self.name = name
        self.data_type = data_type

        if not callable(default):
            orig_default = default
            default = lambda: orig_default
        self.default = default

    def __get__(self, obj, owner=None):
        if self._configured:
            assert owner == self.owner
        if obj is None:
            return self

        if self.key not in obj._item:
            obj._item[self.key] = self.default()

        return obj._item[self.key]

    def __set__(self, obj, value):
        obj._item[self.key] = value

    def __delete__(self, obj):
        raise InvalidRequestError("Unsupport Operation")


class KeyField(Field):
    def map_key(self):
        assert self.attr_type
        return self.attr_type(self.name, data_type=self.data_type)


class HashKeyField(KeyField):
    attr_type = dynamo.HashKey


class RangeKeyField(KeyField):
    attr_type = dynamo.RangeKey


class Index(SchemaBase):
    idx_type = None

    def __init__(self, name=None, parts=None):
        super(Index, self).__init__()
        if not parts:
            raise ArgumentError('Argument `parts` of Index must be specified')
        self.name = name
        self.parts = parts

    def prepare(self):
        parts = []
        for key_cls, field in zip(
                (dynamo.HashKey, dynamo.RangeKey), self.parts):
            key = key_cls(field.name, data_type=field.data_type)
            parts.append(key)
        return dict(name=self.name, parts=parts)

    def map(self):
        return self.idx_type(**self.prepare())


class IncludeIndex(Index):
    def __init__(self, **kwargs):
        self.includes = kwargs.pop('includes', [])
        super(IncludeIndex, self).__init__(**kwargs)

    def prepare(self):
        idx_kwargs = super(IncludeIndex, self).prepare()

        includes = [f.name for f in self.includes]
        idx_kwargs['includes'] = includes

        return idx_kwargs


class LocalIndex(Index):
    pass


class GlobalIndex(Index):
    def __init__(self, **kwargs):
        self.throughput = kwargs.pop('throughput', None)
        super(GlobalIndex, self).__init__(**kwargs)

    def prepare(self):
        idx_kwargs = super(GlobalIndex, self).prepare()

        if self.throughput:
            idx_kwargs['throughput'] = self.throughput
        return idx_kwargs


class LocalAllIndex(LocalIndex):
    idx_type = dynamo.AllIndex


class LocalKeysOnlyIndex(LocalIndex):
    idx_type = dynamo.KeysOnlyIndex


class LocalIncludeIndex(LocalIndex, IncludeIndex):
    idx_type = dynamo.IncludeIndex


class GlobalAllIndex(GlobalIndex):
    idx_type = dynamo.GlobalAllIndex


class GlobalKeysOnlyIndex(GlobalIndex):
    idx_type = dynamo.GlobalKeysOnlyIndex


class GlobalIncludeIndex(GlobalIndex, IncludeIndex):
    idx_type = dynamo.GlobalIncludeIndex
