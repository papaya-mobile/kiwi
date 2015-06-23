# -*- coding: utf-8 -*-

__all__ = ['TableMeta', 'TableBase', 'Table']

from .mapper import setup_mapping

class TableMeta(type):
    '''
    Table setup things here
    '''
    def __init__(cls, name, bases, dict_):
        if is_table(cls):
            setup_mapping(cls, name, dict_)
        type.__init__(cls, name, bases, dict_)

    def __setattr__(cls, name, value):
        type.__setattr__(cls, name, value)

def is_table(cls):
    for base in cls.__bases__:
        if isinstance(base, TableMeta):
            return True
    return False


class TableBase(object):
    '''
    Table API Here
    '''

    @classmethod
    def get(cls, *args):
        '''
            Table.get by keys
        '''
        item = cls.__mapper__.get_item(*args)
        if item is not None:
            return cls(_item=item)
        else:
            return None

    @classmethod
    def query(self, **kwargs):
        return Query(self.__mapper__, **kwargs)

    def _instrument_item(self, item):
        assert isinstance(item, Item)
        self._item = item

    def __init__(self, _item=None, **kwargs):
        item = _item or self.__mapper__.new_item(**kwargs)
        self._instrument_item(item)

    def save(self, overwrite=False):
        assert hasattr(self, '_item')
        self._item.save(overwrite)

    def destroySelf(self):
        assert hasattr(self, '_item')
        self._item.delete()
        # TODO: some another state change

class Table(TableBase):
    __metaclass__ = TableMeta

