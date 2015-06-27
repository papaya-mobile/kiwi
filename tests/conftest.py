# -*- coding: utf-8 -*-

import pytest
import functools

import kiwi


@pytest.fixture(autouse=True)
def clear_metadatas(request):
    def clear():
        for md in kiwi.metadatas:
            md.clear()
    request.addfinalizer(clear)


@pytest.fixture(scope="session")
def local_db():
    from boto.dynamodb2.layer1 import DynamoDBConnection
    conn = DynamoDBConnection(host="localhost", port=8000,
            aws_access_key_id="kiwi", aws_secret_access_key="kiwi",
            is_secure=False)
    return conn

@pytest.fixture
def metadata(request, local_db):
    md = kiwi.MetaData(connection=local_db)

    def teardown():
        md.drop_all()
        md.clear()

    request.addfinalizer(teardown)
    return md

