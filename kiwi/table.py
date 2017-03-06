# -*- coding: utf-8 -*-

from builtins import object
from future.utils import with_metaclass

__all__ = ['TableMeta', 'TableBase', 'Table']

from .mapper import setup_mapping
from .query import Query
from .batch import BatchWrite


def is_table(cls):
    for base in cls.__bases__:
        if isinstance(base, TableMeta):
            return True
    return False


class TableMeta(type):
    '''
    Metaclass/Type of declarative base (that is, ``Table`` here)
    '''
    def __init__(cls, name, bases, dict_):
        if is_table(cls):
            setup_mapping(cls, name, dict_)
        type.__init__(cls, name, bases, dict_)

    ''' Basic Table API
    '''
    def create(tbl):
        '''create the table
        '''
        tbl.__mapper__.create_table()

    def drop(tbl):
        '''drop the table
        '''
        tbl.__mapper__.drop_table()

    def get(tbl, *args):
        ''' Get item by primary key
        '''
        item = tbl.__mapper__.get_item(*args)
        if item is not None:
            return tbl(_item=item)
        else:
            return None

    def batch_get(tbl, keys):
        return tbl.__mapper__.batch_get(keys)

    def batch_write(tbl):
        return BatchWrite(tbl.__mapper__)

    def delete(tbl, **kwargs):
        return tbl.__mapper__.delete_item(**kwargs)

    def query(tbl, **kwargs):
        return Query(tbl.__mapper__, **kwargs)


class TableBase(object):
    ''' Basic Item API
    '''
    def __init__(self, _item=None, **kwargs):
        self._item = self.__mapper__.new_item(_item, **kwargs)

    def save(self, overwrite=False):
        assert hasattr(self, '_item')
        self._item.save(overwrite)

    def destroySelf(self):
        assert hasattr(self, '_item')
        self._item.delete()

    def items(self):
        assert hasattr(self, '_item')
        return self._item.items()


class Table(with_metaclass(TableMeta, TableBase)):
    """
    Base class of declarative class definitions.

    To declare a class mapping to a dynamodb table, simply derive from it.
    """
