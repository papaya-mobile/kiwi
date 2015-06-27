# -*- coding: utf-8 -*-

import pytest

from kiwi import *
from boto.dynamodb2.types import *


def test_basic(metadata):
    class User(Table):
        __metadata__ = metadata
        id = HashKeyField(data_type=NUMBER)
        name = Field()

    metadata.create_all()

    u = User(id=1, name='a')
    assert isinstance(u, User)
    assert u.id is 1
    assert u.name is 'a'
    u.save()

    u = User.get(1)
    assert isinstance(u, User)
    u.name = 'aa'
    u.save()

    u = User.get(1)
    assert isinstance(u, User)
    assert u.name == 'aa'
    u.destroySelf()

    u = User.get(1)
    assert u is None





