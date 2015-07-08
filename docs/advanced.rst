
Advanced Usage
==============

.. _adv-multi-metadata:

Multiple Metadata
-----------------

Usually, the default metadata is enough. In some case, you may need more 
than one metadata to involve different dynamodb endpoint. You can do it 
as following::

    from kiwi import Metadata

    another_metadata = Metadata(connection=connect_to_region())
    
    class User(Table):
        __metadata__ = another_metadata
        ...

inherit table
-------------

You can also inherit tables, for example::

    class UserInfoBase(object):
        __throughput__ = {'read': 2, 'write': 1}

        user_id = HashKeyField(data_type=NUMBER)
        

    class UserTask(Table, UserInfoBase):
        task_id = RangeKeyField()
        
    class UserName(Table, UserInfoBase):
        name = Field()
        

custom table api
----------------

At some time you may want to custom table api, just declare a customised 
``Table``::

    from kiwi import TableBase
    from kiwi import TableMeta

    class MyTableBase(TableBase):
        # custom the api
        ...

    class MyTable(with_metaclass(TableMeta, TableBase)):
        pass

    class User(MyTable):
        ...


custom Dynamizer
----------------
``Dynamizer`` is a transfer between python type and dynamodb API types.
The default dynamizer is `boto.dynamodb.types.Dynamizer`_. However, at some
times you may want to use a customized dynamizer. It's simple::

    from boto.dynamodb2.types import Dynamizer
    from kiwi import metadata

    class MyDynamizer(Dynamizer):
        ...

     metadata.configure(dynamizer=MyDynamizer)

.. Caution::
    ``boto.dynamodb2`` **dose not** provide public interface to custom 
    dynamizer for tables. Kiwi provides this by accessing the priavte 
    property ``boto.dynamodb2.table.Table._dynamizer``. So use this feature 
    at your own risk!

.. _boto.dynamodb.types.Dynamizer: https://boto.readthedocs.org/en/latest/ref/dynamodb.html#boto.dynamodb.types.Dynamizer
