# -*- coding: utf-8 -*-

__all__ = ['Field', 'HashKeyField', 'RangeKeyField', 'Attribute',
        'Index', 'LocalIndex', 'GlobalIndex', 'IncludeIndex',
        'LocalAllIndex', 'LocalKeysOnlyIndex', 'LocalIncludeIndex',
        'GlobalAllIndex', 'GlobalKeysOnlyIndex', 'GlobalIncludeIndex',
        ]

from . import dynamo

class Attribute(object):
    def __init__(self, class_, key,
            attr_type=None, data_type=None, default=None):
        self.class_ = class_
        self.key = key
        self.attr_type = attr_type
        self.data_type = data_type

        if not callable(default):
            orig_default = default
            default = lambda : orig_default
        self.default = default

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self

        if self.key in obj._item:
            return obj._item[self.key]
        return self.default()

    def __set__(self, obj, value):
        obj._item[self.key] = value

    def __delete__(self, obj):
        del obj._item[self.key]


class Field(object):
    attr_type = None
    def __init__(self, name=None, data_type=dynamo.STRING, default=None):
        self.name = name
        self.data_type = data_type
        self.default = default

    def map(self, cls_):
        return Attribute(cls_,
                key=self.name,
                attr_type=self.attr_type,
                data_type=self.data_type,
                default=self.default)

class KeyField(Field):
    def map_key(self):
        assert self.attr_type
        return self.attr_type(self.name, data_type=self.data_type)

class HashKeyField(KeyField):
    attr_type = dynamo.HashKey

class RangeKeyField(KeyField):
    attr_type = dynamo.RangeKey


class Index(object):
    idx_type = None

    def __init__(self, name=None, parts=None):
        if not parts:
            raise Exception('Index parts not specified')
        self.name = name
        self.parts = parts

    def prepare(self):
        parts = []
        for key_cls, field in zip((dynamo.HashKey, dynamo.RangeKey), self.parts):
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

        includes = [ f.name for f in self.includes]
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

