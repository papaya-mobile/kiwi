# -*- coding: utf-8 -*-

from builtins import map

__version__ = '0.0.1'
VERSION = tuple(map(int, __version__.split(".")))

from .table import *
from .field import *
from .mapper import *
from .metadata import *
from .query import *
from .exceptions import *


__all__ = sum(map(lambda m: getattr(m, '__all__', []),
                  [table, field, mapper, metadata, query, exceptions],
                  ), [])

metadata = MetaData()
metadatas = set()

__all__ += ['metadata', 'metadatas']
