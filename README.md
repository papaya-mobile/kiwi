# KIWI - DynamoDB ORM for Python

Kiwi is an AWS DynamoDB ORM for Python. Kiwi depends on boto and supports Python2.7, Python3.4 at least.

### Installation

Install Kiwi from source:

    $ git clone git://github.com/papaya-eng/kiwi.git
    $ cd kiwi
    $ python setup.py sdist
    $ pip install dist/kiwi-x.x.x.tar.gz

### Basic usage

The ```table```, ```Field``` of Kiwi will be used to define a DynamoDB Model:

    from boto.dynamodb2 import connect_to_region
    from boto.dynamodb2.types import NUMBER
    from kiwi import metadata
    from kiwi import Table
    from kiwi import Field
    from kiwi import HashKeyField, RangeKeyField
    
    connection = connect_to_region("us-east-1")
    metadata.configure(connection=connection)
    
    class User(Table):
        id = HashKeyField(data_type=NUMBER)
        name = Field()
    
    class UserTask(Table):
        user_id = HashKeyField(data_type=NUMBER)
        task_id = RangeKeyField()
        name = Field()
        time = Field()
        done = Field(data_type=NUMBER, default=0)

After define a model, it's very easy to insert items:

    >>> User(id=1, name='Aaron').save()
    >>> UserTask(user_id=1, task_id='first', name='test').save()
    >>> UserTask(user_id=1, task_id='second', name='test').save()     
    >>> UserTask(user_id=1, task_id='fifth', name='test').save()

You may also get and query items easily:

    >>> User.get(1)
    <User object at 0x7f515343ccd0>
    >>> UserTask.get(1, 'second')
    <UserTask object at 0x7f515343c950> 
    
    >>> UserTask.query().filter(UserTask.user_id==1, UserTask.task_id.beginswith_('f')).all()
    [<UserTask object at 0x7f51533c9810>, <UserTask object at 0x7f51533c9790>]


### Documentation

You can read the docs online: http://papaya-kiwi.readthedocs.org/en/latest/

You can also generate documentation by yourself. 
The docs are created by sphinx, which can be installed via pip.

	pip install sphinx
	cd kiwi/docs
	make html
