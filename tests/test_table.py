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
        assert set(batch_get_keys([1, 2, 3])) == set([1, 2, 3])
        assert set(batch_get_keys([2, 8])) == set([2, 8])
        assert set(batch_get_keys([5, 13])) == set([5])
        assert set(batch_get_keys([222, 123])) == set()

        with pytest.raises(ArgumentError):
            User.batch_get([(2, 2)])

    def test_batch_get_2(self, UserAction):
        def batch_get_keys(keys):
            return [(u.id, u.time) for u in UserAction.batch_get(keys)]

        assert set(batch_get_keys([])) == set()
        assert set(batch_get_keys([(1, 1),  (2, 2)])) == set([(1, 1), (2, 2)])
        assert set(batch_get_keys([(3, 5)])) == set([(3, 5)])
        assert set(batch_get_keys([(4, 1222),  (4, 12)])) == set([(4, 12)])
        assert set(batch_get_keys([(222, 123)])) == set()

        with pytest.raises(ArgumentError):
            UserAction.batch_get([2, 2])

    def test_batch_write_1(self, User):
        with User.batch_write() as batch:
            pass

        with User.batch_write() as batch:
            batch.add(User(id=100, name='100'))
            batch.add(User(id=101, name='101'))
            batch.add(User(id=102, name='102'))
            batch.add(User(id=103, name='103'))
            with pytest.raises(ArgumentError):
                batch.add(234)

        keys = range(100, 104)
        assert set([u.id for u in User.batch_get(keys)]) == set(keys)

        with User.batch_write() as batch:
            batch.delete({'id': 100})
            batch.delete(User(id=101))

            with pytest.raises(ArgumentError):
                batch.delete(102)
            with pytest.raises(ArgumentError):
                batch.delete({'name': '103'})

        assert set([u.id for u in User.batch_get(keys)]) == set([102, 103])

        with User.batch_write() as batch:
            batch.add(User(id=100, name='100'))
            batch.delete(dict(id=102))
            batch.delete(dict(id=103))

        assert set([u.id for u in User.batch_get(keys)]) == set([100])

        User.delete(id=100)
        assert set([u.id for u in User.batch_get(keys)]) == set()

    def test_batch_write_2(self, UserAction):
        UA = UserAction

        with UA.batch_write() as batch:
            pass

        with UA.batch_write() as batch:
            batch.add(UA(id=100, time=100, name='100'))
            batch.add(UA(id=101, time=101, name='101'))
            batch.add(UA(id=102, time=102, name='102'))
            batch.add(UA(id=103, time=103, name='103'))

            with pytest.raises(ArgumentError):
                batch.add(123444)

        keys = zip(range(100, 104), range(100, 104))
        assert set([(u.id, u.time) for u in UA.batch_get(keys)]) == set(keys)

        with UA.batch_write() as batch:
            batch.delete(UA(id=100, time=100))
            batch.delete({'id': 101, 'time': 101})

            with pytest.raises(ArgumentError):
                batch.delete(102)
            with pytest.raises(ArgumentError):
                batch.delete({'time': 103})

        assert set([(u.id, u.time) for u in UA.batch_get(keys)]
                   ) == set([(102, 102), (103, 103)])

        with UA.batch_write() as batch:
            batch.delete({'id': 102, 'time': 102})
            batch.add(UA(id=100, time=100, name='100'))
            batch.delete({'id': 103, 'time': 103})

        assert set([(u.id, u.time) for u in UA.batch_get(keys)]
                   ) == set([(100, 100)])

        UA.delete(id=100, time=100)
        assert set([(u.id, u.time) for u in UA.batch_get(keys)]) == set()
