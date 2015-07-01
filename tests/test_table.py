# -*- coding: utf-8 -*-

import pytest

from kiwi import *
from boto.dynamodb2.types import *


class TestTable(object):
    def test_hashkey(self, User):
        u = User(id=11, name='a')
        assert isinstance(u, User)
        assert u.id is 11
        assert u.name is 'a'
        u.save()

        u = User.get(11)
        assert isinstance(u, User)
        u.name = 'aa'
        u.save()

        u = User.get(11)
        assert isinstance(u, User)
        assert u.name == 'aa'
        u.destroySelf()

        u = User.get(11)
        assert u is None

    def test_hash_range(self, UserAction):
        u = UserAction(id=11, time=1111, name='bbbb')
        assert isinstance(u, UserAction)
        assert u.id is 11
        assert u.time is 1111
        assert u.name == 'bbbb'
        assert u.duration is None
        u.save()

        u = UserAction.get(11, 1111)
        assert isinstance(u, UserAction)
        u.name = 'cccc'
        u.save()

        u = UserAction.get(11, 1111)
        assert isinstance(u, UserAction)
        assert u.name == 'cccc'
        u.destroySelf()

        u = UserAction.get(11, 1111)
        assert u is None


    def test_batch_get_1(self, User):
        def batch_get_keys(keys):
            return [u.id for u in User.batch_get(keys)]

        assert set(batch_get_keys([])) == set()
        assert set(batch_get_keys([1,2,3])) == set([1,2,3])
        assert set(batch_get_keys([2, 8])) == set([2,8])
        assert set(batch_get_keys([5,13])) == set([5])
        assert set(batch_get_keys([222, 123])) == set()

        with pytest.raises(ArgumentError):
            User.batch_get([(2,2)])

    def test_batch_get_2(self, UserAction):
        def batch_get_keys(keys):
            return [(u.id, u.time) for u in UserAction.batch_get(keys)]

        assert set(batch_get_keys([])) == set()
        assert set(batch_get_keys([(1,1), (2,2)])) == set([(1,1),(2,2)])
        assert set(batch_get_keys([(3,5)])) == set([(3,5)])
        assert set(batch_get_keys([(4,1222), (4,12)])) == set([(4,12)])
        assert set(batch_get_keys([(222, 123)])) == set()

        with pytest.raises(ArgumentError):
            UserAction.batch_get([2,2])

