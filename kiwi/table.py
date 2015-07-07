# -*- coding: utf-8 -*-

from builtins import object
from future.utils import with_metaclass

__all__ = ['TableMeta', 'TableBase', 'Table']

from .mapper import setup_mapping
from .query import Query
from .batch import BatchWrite


class TableMeta(type):
    '''
    Metaclass/Type of declarative base (that is, ``Table`` here)
    '''
    def __init__(cls, name, bases, dict_):
        if is_table(cls):
            setup_mapping(cls, name, dict_)
        type.__init__(cls, name, bases, dict_)


def is_table(cls):
    for base in cls.__bases__:
        if isinstance(base, TableMeta):
            return True
    return False


class TableBase(object):
    '''
    Basic Table API
    '''
    @classmethod
    def create(cls):
        '''create the table
        '''
        cls.__mapper__.create_table()

    @classmethod
    def drop(cls):
        '''drop the table
        '''
        cls.__mapper__.drop_table()

    @classmethod
    def get(cls, *args):
        ''' Get item by primary key
        '''
        item = cls.__mapper__.get_item(*args)
        if item is not None:
            return cls(_item=item)
        else:
            return None

    @classmethod
    def batch_get(cls, keys):
        return cls.__mapper__.batch_get(keys)

    @classmethod
    def batch_write(cls):
        return BatchWrite(cls.__mapper__)

    @classmethod
    def delete(cls, **kwargs):
        return cls.__mapper__.delete_item(**kwargs)

    @classmethod
    def query(self, **kwargs):
        return Query(self.__mapper__, **kwargs)

    def __init__(self, _item=None, **kwargs):
        item = _item or self.__mapper__.new_item(**kwargs)
        self._item = item

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
