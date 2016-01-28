# -*- coding: utf-8 -*-

from builtins import map

__version__ = '0.0.3'

from .table import *
from .field import *
from .metadata import *
from .query import *
from .exceptions import *


__all__ = sum(map(lambda m: getattr(m, '__all__', []),
                  [table, field, metadata, query, exceptions],
                  ), [])

metadata = MetaData()
metadatas = set()

def create_all():
    for md in metadatas:
        md.create_all()

def drop_all():
    for md in metadatas:
        md.drop_all()


__all__ += ['metadata', 'create_all', 'drop_all']
