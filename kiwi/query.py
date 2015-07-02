# -*- coding: utf-8 -*-

__all__ = ['Query']

from .field import Expression, Index
from .exceptions import *


class Query(object):
    def __init__(self, mapper, index=None,
                 attributes=None, consistent=False,
                 max_page_size=None, reverse=False, limit=None):
        self._mapper = mapper

        self._index = self._check_index(index)
        self._attributes = self._check_attributes(attributes)

        self._consistent = consistent
        self._max_page_size = max_page_size

        self._reverse = reverse
        self._limit = limit
        self._filters = []

        self._fired = False

    def clone(self):
        cloned = Query(mapper=self._mapper)
        cloned._index = self._index
        if self._attributes:
            cloned._attributes = self._attributes[:]
        cloned._consistent = self._consistent
        cloned._max_page_size = self._max_page_size
        cloned._reverse = self._reverse
        cloned._limit = self._limit
        cloned._filters = self._filters[:]
        return cloned

    def _check_index(self, index):
        if index is None:
            return index
        if isinstance(index, Index):
            if index.owner is self._mapper.class_:
                return index.name
            else:
                raise ArgumentError("The index is not owned by "
                                    "Table %s" % self._mapper.tablename)
        if index in self._mapper.indexes:
            return index
        if index in self._mapper.global_indexes:
            return index
        raise ArgumentError("Unknown index `%s`" % index)

    def _check_attributes(self, attrs):
        if not attrs:
            return None
        else:
            return [f.name for f in attrs]

    def _build_filters(self):
        query_filter = []
        filter_kwargs = []

        if self._index:
            idx = getattr(self._mapper.class_, self._index)
            keyfields = idx.parts
        else:
            keyfields = self._mapper.schema
        keyfields = [f.name for f in keyfields]

        def involve_keyfield(exp):
            return exp.field.name in keyfields

        for exp in self._filters:
            if involve_keyfield(exp):
                filter_kwargs.append(exp.schema())
            else:
                query_filter.append(exp.schema())

        query_filter = dict(query_filter)
        filter_kwargs = dict(filter_kwargs)
        return query_filter, filter_kwargs

    def _fire(self):
        self._fired = True

        query = self._mapper.table.query_2
        query_filter, filter_kwargs = self._build_filters()

        if not filter_kwargs:
            raise InvalidRequestError("Hashkey must be involved into filters")

        results = query(limit=self._limit,
                        index=self._index,
                        reverse=self._reverse,
                        consistent=self._consistent,
                        attributes=self._attributes,
                        max_page_size=self._max_page_size,
                        query_filter=query_filter,
                        conditional_operator=None,
                        **filter_kwargs)
        return self._mapper.wrap_result(results)

    def __iter__(self):
        return self._fire()

    def filter(self, *args):
        assert not self._fired
        for exp in args:
            if not isinstance(exp, Expression):
                raise ArgumentError("filter must be  an Expression")
            self._filters.append(exp)
        return self

    def asc(self):
        assert not self._fired
        self._reverse = False
        return self

    def desc(self):
        assert not self._fired
        self._reverse = True
        return self

    def limit(self, limit):
        assert not self._fired
        self._limit = limit
        return self

    def count(self):
        assert not self._fired
        return len(list(self))

    def all(self):
        return list(self)

    def first(self):
        self = self.limit(1)
        ret = list(self)
        return ret[0] if ret else None
