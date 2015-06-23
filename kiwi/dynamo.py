# -*- coding: utf-8 -*-

'''
all boto.dynamodb2 import comes here
'''

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import (HashKey, RangeKey,
                                   AllIndex, KeysOnlyIndex, IncludeIndex,
                                   GlobalAllIndex, GlobalKeysOnlyIndex,
                                   GlobalIncludeIndex)
from boto.dynamodb2.items import Item
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.results import ResultSet, BatchGetResultSet
from boto.dynamodb2.types import (NonBooleanDynamizer, Dynamizer, FILTER_OPERATORS,
                                  QUERY_OPERATORS, STRING)

from boto.exception import JSONResponseError
from boto.dynamodb2.exceptions import (DynamoDBError,
        ItemNotFound,
        )
