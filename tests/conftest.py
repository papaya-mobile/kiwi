# -*- coding: utf-8 -*-

import pytest
import functools

import kiwi


def pytest_runtest_teardown(item, nextitem):
    for md in kiwi.metadatas:
        md.clear()
