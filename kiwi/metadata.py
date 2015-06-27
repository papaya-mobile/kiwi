# -*- coding: utf-8 -*-

__all__ = ['MetaData']

import re
from threading import RLock

import kiwi

def _default_tablename_generator(cls):
    clsname = cls.__name__
    return re.sub('([^A-Z])([A-Z])', '\\1_\\2', clsname).lower()


class MetaData(object):

    def __init__(self, connection=None, tablename_generator=None, throughput=None):
        self.connection = connection or None
        self.generate_tablename = tablename_generator or _default_tablename_generator
        self.throughput = throughput or None

        self._lock = RLock()
        self._unconfigurable = False
        self._tables = {} # tablename: mapper

    def configure(self, connection=None, tablename_generator=None, throughput=None):
        with self._lock:
            if self._unconfigurable:
                raise Exception("unconfigurable metadata")
            if connection is not None:
                self.connection = connection or None
            if tablename_generator is not None:
                self.generate_tablename = tablename_generator or _default_tablename_generator
            if throughput is not None:
                self.throughput = throughput or None


    def add(self, mapper):
        with self._lock:
            self._unconfigurable = True

            if mapper.tablename in self._tables:
                raise Exception("multi table with same tablename: %s" % mapper.tablename)

            mapper.tablename = mapper.tablename or self.generate_tablename(mapper.class_)
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


