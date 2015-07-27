from builtins import object
# -*- coding: utf-8 -*-

__all__ = ['Query']

from .field import Index
from .expression import Expression
from .exceptions import *


class Query(object):
    def __init__(self, mapper, index=None,
                 attributes=None, consistent=False,
                 max_page_size=None, reverse=False, limit=None):
        self._mapper = mapper

        self._index = self._check_index(index)
        self._attributes = self._check_attributes(attributes)

        if self._index:
            idx = getattr(self._mapper.class_, self._index)
            keyfields = idx.parts
        else:
            keyfields = self._mapper.schema
        self._keyfields = [f.name for f in keyfields]

        self._consistent = consistent
        self._max_page_size = max_page_size

        self._reverse = reverse
        self._limit = limit

        self._key_conds = []
        self._filters = []

        self._fired = False

    def clone(self):
        cloned = Query(mapper=self._mapper)
        cloned._index = self._index
        if self._attributes:
            cloned._attributes = self._attributes[:]
        cloned._keyfields = self._keyfields[:]
        cloned._consistent = self._consistent
        cloned._max_page_size = self._max_page_size
        cloned._reverse = self._reverse
        cloned._limit = self._limit
        cloned._key_conds = self._key_conds[:]
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

    def _build_raw_filters(self, filters):
        return dict(map(lambda exp: exp.schema(), filters))

    def _fire(self):
        self._fired = True

        query = self._mapper.table.query_2

        query_filter = self._build_raw_filters(self._filters)
        filter_kwargs = self._build_raw_filters(self._key_conds)

        if not filter_kwargs:
            raise InvalidRequestError("Key Condition should be specified "
                                      "via the method `query.onkeys`")

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

    def onkeys(self, hashkey_cond, rangekey_cond=None):
        ''' Specify KeyConditionExpression
        '''
        assert not self._fired
        if self._key_conds:
            raise InvalidRequestError("Key Conditions have been specified")

        if not isinstance(hashkey_cond, Expression):
            raise ArgumentError("hashkey_cond should be an Expression")
        if hashkey_cond.field.name != self._keyfields[0]:
            msg = "The hashkey_cond has a wrong field `%s`, "\
                  "which should be `%s`"
            msg = msg % (hashkey_cond.field.name, self._keyfields[0])
            raise ArgumentError(msg)
        if not hashkey_cond.is_eq():
            raise ArgumentError("hashkey_cond should be an equal operation")

        if rangekey_cond:
            if not isinstance(rangekey_cond, Expression):
                raise ArgumentError("rangekey_cond should be an Expression")
            if rangekey_cond.field.name != self._keyfields[1]:
                msg = "The rangekey_cond has a wrong field `%s`, "\
                      "which should be `%s`"
                msg = msg % (rangekey_cond.field.name, self._keyfields[1])
                raise ArgumentError(msg)
            if not rangekey_cond.is_key_condition():
                msg = "The condtion `%s` should not apply to a range key"
                msg = msg % rangekey_cond.op
                raise ArgumentError(msg)

        self._key_conds.append(hashkey_cond)
        if rangekey_cond:
            self._key_conds.append(rangekey_cond)

        return self

    def filter(self, *args):
        ''' filter on another keys
        '''
        assert not self._fired
        for exp in args:
            if not isinstance(exp, Expression):
                raise ArgumentError("filter must be an Expression")
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
