# -*- coding: utf-8 -*-

__all__ = ['Query']

class Query(object):
    def __init__(self, mapper, **kwargs):
        self.mapper = mapper

        #TODO: setup filters

        self._fired = False

    def __iter__(self):
        self._fired = True
        for item in self.mapper.query(xxxx):
            yield self.mapper.class_(_item=item)

    def filter(self, **kwargs):
        assert not self._fired
        #TODO: setup filters

    def limit(self, limit):
        assert not self._fired
        self.limit = limit

    def count(self):
        assert not self._fired
        return len(list(self))

    def all(self):
        return list(self)

    def first(self):
        self = self.limit(1)
        ret = list(self)
        return ret[0] if ret else None

