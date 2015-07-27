
Quick Start
===========

declare a table
---------------

Let's begin with this simple example::

    from boto.dynamodb2 import connect_to_region
    from boto.dynamodb2.types import NUMBER
    from kiwi import metadata
    from kiwi import Table
    from kiwi import Field
    from kiwi import HashKeyField

    metadata.configure(connection=connect_to_region("us-east-1"))
    
    class User(Table):
        id = HashKeyField(data_type=NUMBER)
        name = Field()
        birth = Field(data_type=NUMBER, default=0)

In this example, we declare a class ``User`` which maps to a dynamodb table
named ``user``. Let's go through it step by step.

metadata 
++++++++

``metadata`` is a collection of dynamodb configure, includes connection,
default tablename factory and default throughput.

Before declaring tables, we need configure the metadata by using 
``metadata.configure``.

Besides the default metadata instance, you can also create ``Metadata`` 
instance for more than one dynamodb endpoint or configure, see 
:ref:`adv-multi-metadata`.

tablename
+++++++++
Defaultly, the tablename in dynamodb is generated as::

    re.sub('([^A-Z])([A-Z])', '\\1_\\2', cls.__name__).lower()

That is, class ``User`` is mapped to dynamodb table ``user`` in the above 
example. And a class named ``UserTask`` will be mapped to the table named 
``user_task``.

You can define your own tablename generator and apply it via 
``metadata.configure``::

    def my_tablename_factory(cls):
        return cls.__name__

    metadata.configure(tablename_factory=my_tablename_factory)

Also, you can assign a special tablename for each Table::

    class User(Table):
        __tablename__ = 'users'
        ...


throughput
++++++++++
Another dynamodb configure is ``throughput``.  As the same, you can
config it globally via ``metadata``::

    metadata.configure(throughput={'read':5, 'write':5})

or specially on each table::

    class User(Table):
        __throughput__ = {'read':20, 'write': 30}
        ...


Field
+++++
``Field`` represents a field in the record of a table. You can define
its name, data_type, and default value.

``KeyField``, which include ``HashKeyField`` and ``RangeKeyField``, 
represents the primary key field of the dynamodb table. Dynamodb table
must have primary key, either a hash key, or combined hash-range key.
So you must provide a ``HashKeyField`` at least.


Index
+++++
To declare a dynamodb ``Secordary Index``, you can use ``Index``::

    from kiwi import GlobalAllIndex, LocalAllIndex

    class UserTask(Table):
        user_id = HashKeyField(data_type=NUMBER)
        task_id = RangeKeyField()
        name = Field()
        time = Field()
        done = Field()
        ...
        local_index = LocalAllIndex(parts=[user_id, name])
        global_index = GlobalAllIndex(parts=[task_id, time])
        ...


create and drop tables
----------------------
To create and drop tables, you can use the two util ``create_all``, 
``drop_all``::

    from kiwi import create_all
    from kiwi import drop_all

    # table declarative
    class User(Table):
        ...

    create_all()

    drop_all()

The two util will create or drop tables in all metadatas you declare.
To create/drop tables in a metadata, use ``Metadata.create_all``, 
``Metadata.drop_all``.

To create/drop a special table, use ``Table.create`` and ``Table.drop``.


basic read and write operation
------------------------------

Assume you have declared the two tables::
    
    class User(Table):
        id = HashKeyField(data_type=NUMBER)
        name = Field()
        birth = Field(data_type=NUMBER, default=0)

    class UserTask(Table):
        user_id = HashKeyField(data_type=NUMBER)
        task_id = RangeKeyField()
        name = Field()
        time = Field()
        done = Field()
        

create an item
++++++++++++++

Instantiate a declarative class to create an item::

    u = User(id=1, name='test')
    task = UserTask(user_id=1, task_id='a', name='test')

and use ``Table.save`` to write the item into dynamodb::

    u.save()
    task.save()

get an item
+++++++++++

You can read a item from dynamodb by primary key::

    u = User.get(1)
    assert u.name == 'test'
    task = UserTask.get(1, 'a')
    assert task.name == 'test'

modify fields
+++++++++++++

Simply use assign operation to modify fields::
    
    u.name = 'modified'
    u.save()
    task.done = True
    task.save()

Remeber using ``save`` method to flush changes into dynamodb.

delete an item
++++++++++++++

Use ``destroySelf`` method to delete an item::

    u.destroySelf()
    u = User.get(1)
    assert u is None


query
-----

For hash-range primary key tables, you can do a query operation::

    query = UserTask.query().onkeys(
        UserTask.user_id==1, UserTask.task_id.beginswith_('a'))

After create a ``query`` instance, ``onkeys`` method must be called to
specify the primary key condition. The method accepts two expression at 
most: the first is the condition on hash key, and the second is the 
condition on range key, which can be ignored.  Note that

1. ``onkeys`` must be called once and only once,
2. the hash key condition must be a ``==`` operation,
3. the range key condition only supports ``==``, ``<``, ``<=``, ``>``, 
   ``>=``, ``between_``, ``beginswith_``.

Then you can add filters on arbitrary field use ``filter`` method::

    query.filter(UserTask.time > 100)


And you can modify the query by call its methods::

    # more filter
    query.filter(UserTask.done == 1)

    # reverse the order
    query.desc()

    # limit max return items
    query.limit(10)

However, all above operations would not tigger a real db query, then are 
all `in-memory` operations.

To tigger real db queries, try to get the query result by using these 
methods::

    # get all items
    items = query.all()

    # or as an iterator
    for item in query:
        ...

    # only get the first item
    query.first()

    # only get the count:
    query.count()

Remeber that the query can only be fired only once. To use the query 
multiple times, try to clone a new one::

    # clone an unfired query from even an fired one
    query = query.clone()


query on index
++++++++++++++

You can query on secondary index::

    class UserAction(Table):
        ...
        local_index = LocalAllIndex(parts=[user_id, name])
        global_index = GlobalAllIndex(parts=[task_id, time])
        ...

    # use secondary local index
    query = UserAction.query(index=UserAction.local_index)
    query.onkeys(UserAction.user_id==1, UserAction.name.beginswith_('t'))

    # use secondary global index
    query = UserAction.query(index=UserAction.global_index)
    query.onkeys(UserAction.task_id==20, UserAction.time <= 20)


some notice
+++++++++++

- You can only query on hash-range primary key table.
- The hashkey in the query must be in an ``equal`` condition.


batch read and write
--------------------

batch read
++++++++++

You can use ``Table.batch_get`` to read multiple items at once::

    User.batch_get([1,2,3])
    UserAction.batch_get([(1,'a'), (2,'b')])

batch write
+++++++++++

Yan can use ``Table.batch_write`` to add and/or delete multiple items::

    with User.batch_write() as batch:
        batch.add(User(id=100, name='100'))
        batch.add(User(id=101, name='101'))

        batch.delete({'id': 200})
        batch.delete(User(id=201))

When leaving the context, the changes would be flush into dynamodb.

