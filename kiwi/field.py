# -*- coding: utf-8 -*-

__all__ = ['Field', 'FieldType', 'Index', 'Attribute']

from . import dynamo

class Attribute(object):
    def __init__(self, class_, key, default=None):
        self.class_ = class_
        self.key = key

        if not callable(default):
            default = lambda : default
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


class FieldType(object):
    HASH, RANGE, NORMAL = range(3)

class Field(object):
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', None)
        self.attr_type = kwargs.get('attr_type', FieldType.NORMAL)
        self.data_type = kwargs.get('data_type', dynamo.STRING)
        self.default = kwargs.get('default', None)

    def map(self, cls_):
        return Attribute(cls_, key=self.name, default=self.default)


# Index types
INDEX_TYPES = {
        'local_all' : dynamo.AllIndex,
        'local_keysonly' : dynamo.KeysOnlyIndex,
        'local_include': dynamo.IncludeIndex,
        'global_all' : dynamo.GlobalAllIndex,
        'global_keysonly' : dynamo.GlobalKeysOnlyIndex,
        'global_include' : dynamo.GlobalIncludeIndex,
    }

class Index(object):
    def __init__(self, name, type, parts, **kwargs):
        self.name = name
        if type not in INDEX_TYPES:
            raise Exception('Unknown index type')
        self.type = type
        self.parts = parts
        self.index_kwargs = kwargs

    def map(self):
        type_cls = INDEX_TYPES[self.type]

        parts = []
        for key_cls, field in zip((dynamo.HashKey, dynamo.RangeKey), self.parts):
            key = key_cls(field.name, field.type)
            parts.append(key)

        return type_cls(self.name, parts, **self.index_kwargs)

