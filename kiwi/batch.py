# -*- coding: utf-8 -*-

from . import dynamo
from .exceptions import *

class BatchWrite(object):
    def __init__(self, mapper):
        self._mapper = mapper
        self._class = mapper.class_


    def __enter__(self):
        self._batchtable = dynamo.BatchTable(self._mapper.table)
        return self

    def add(self, item):
        if not isinstance(item, self._class):
            raise ArgumentError("item is not an instance of %s" % self._mapper.tablename)
        self._batchtable.put_item(dict(item.items()))

    def delete(self, item):
        if isinstance(item, self._class):
            kwargs = {}
            for key in self._mapper.schema:
                kwargs[key.name] = getattr(item, key.name)
        elif isinstance(item, dict):
            kwargs = item
            for key in self._mapper.schema:
                if key.name not in kwargs:
                    raise ArgumentError("Primary key is NOT integral")
        else:
            raise ArgumentError("Invalid type of argument")
        self._batchtable.delete_item(**kwargs)

    def __exit__(self, type, value, traceback):
        self._batchtable.__exit__(type, value, traceback)

