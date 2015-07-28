# -*- coding: utf-8 -*-

import os
import re
import codecs
from setuptools import setup

ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

with codecs.open(
        os.path.join(ROOT, 'kiwi', '__init__.py'), 'r', 'utf-8') as init:
    try:
        version = VERSION_RE.search(init.read()).group(1)
    except IndexError:
        raise RuntimeError('Unable to determine version.')


setup(name='kiwi',
      version=version,
      packages=['kiwi'],
      description='Simple dynamodb ORM',
      author='Papaya Backend',
      author_email='backend@papayamobile.com',
      install_requires=['future', 'boto>=2.38.0'],
      )
