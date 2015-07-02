# -*- coding: utf-8 -*-

__all__ = ['MetaData']

import re
from threading import RLock

import kiwi
from .exceptions import *


def _tablename_factory(cls):
    clsname = cls.__name__
    return re.sub('([^A-Z])([A-Z])', '\\1_\\2', clsname).lower()


class MetaData(object):
    def __init__(self, connection=None,
                 tablename_factory=None, throughput=None):
        self.connection = connection or None
        self.throughput = throughput or None
        self.tablename_factory = _tablename_factory
        if tablename_factory:
            self.tablename_factory = tablename_factory

        self._lock = RLock()
        self._unconfigurable = False
        self._tables = {}   # tablename: mapper

    def configure(self, connection=None,
                  tablename_factory=None, throughput=None):
        with self._lock:
            if self._unconfigurable:
                raise InvalidRequestError("The metadata can NOT be "
                                          "configured after some table "
                                          "has been attached")
            if connection is not None:
                self.connection = connection or None
            if tablename_factory is not None and tablename_factory:
                self.tablename_factory = tablename_factory
            if throughput is not None:
                self.throughput = throughput or None

    def add(self, mapper):
        with self._lock:
            self._unconfigurable = True

            if mapper.tablename in self._tables:
                raise InvalidRequestError("Table with name `%s` has "
                                          "been added!" % mapper.tablename)

            if not mapper.tablename:
                mapper.tablename = self.tablename_factory(mapper.class_)
            mapper.throughput = mapper.throughput or self.throughput
            self._tables[mapper.tablename] = mapper

            kiwi.metadatas.add(self)

    def __contains__(self, mapper):
        return mapper.tablename in self._tables

    def remove(self, mapper):
        with self._lock:
            self._tables.pop(mapper.tablename, None)

    def clear(self):
        with self._lock:
            self._tables.clear()

    def __iter__(self):
        return iter(self._tables)

    def items(self):
        return self._tables.items()

    def values(self):
        return self._tables.values()

    def create_all(self):
        for m in self.values():
            m.create_table()

    def drop_all(self):
        for m in self.values():
            m.drop_table()
