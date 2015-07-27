# -*- coding: utf-8 -*-

'''
DynamoDB KeyConditionExpression and FilterExpression
'''

__all__ = ['Filterable', 'Expression']

from builtins import object

from . import dynamo


class Filterable(object):
    def __eq__(self, other):
        return Expression(self, 'eq', other)

    def __lt__(self, other):
        return Expression(self, 'lt', other)

    def __le__(self, other):
        return Expression(self, 'lte', other)

    def __gt__(self, other):
        return Expression(self, 'gt', other)

    def __ge__(self, other):
        return Expression(self, 'gte', other)

    def between_(self, left, right):
        return Expression(self, 'between', (left, right))

    def beginswith_(self, prefix):
        return Expression(self, 'beginswith', prefix)

    def __ne__(self, other):
        return Expression(self, 'ne', other)

    def in_(self, other):
        return Expression(self, 'in', other)

    def notnone_(self):
        return Expression(self, 'nnull', None)

    def isnone_(self):
        return Expression(self, 'null', None)

    def contains_(self, other):
        return Expression(self, 'contains', other)

    def notcontains_(self, other):
        return Expression(self, 'ncontains', other)


class Expression(object):
    def __init__(self, field, op, other):
        assert isinstance(field, Filterable)
        self.field = field
        self.op = op
        self.other = other

    def schema(self):
        return ('%s__%s' % (self.field.name, self.op), self.other)

    def is_eq(self):
        return self.op == 'eq'

    def is_key_condition(self):
        return self.op in dynamo.QUERY_OPERATORS
