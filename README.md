
Declarative Mapper for DynamoDB

A declarative layer on top of boto.dynamodb2

like Elixir on SQLAlchemy

## API

```
# from Elixir import Entity, Field, DateTime, Unicode, Integer, Boolean, setup_all, create_all, session, metadata
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
```
- Table/Base/Entity
- Column/Field
- data type: DateTime, String, Integer, Boolean
- dynamodb key: HashKey, RangeKey
- utils like: setup_all, create_all, etc

## Feature
- polymorphic/inheritance ??
- automaticly map table name
- local secondary index support
- PEP8
- Abstract Table

## notes

- Dynamodb tables have no relationship, they are not related to each other

## Usage

class User(Table):
    __tablename__ = 'xxxx' # can be ignore, which a default table name generator
    __throughput__ = { 'read': 5, 'write': 5}
    
    hkey = HashKey()
    rkey = RangeKey()


    a = Attr(type=Integer, default=0)
    b = Attr(type=String, default='')

    aindex = GlobalAllIndex('index_name', hkey, a)

u = User()
u.save()

u.a = 12
u.save()

u = User.get(hkey=xx, rkey=xx)
type(u) == User 

User.query.filter(User.a=xxx)

query.all()
query.count()
query.delete()
query.limit()

query.__iter__

class ActionUser(User):
    __tablename__ = 'yyyy'



