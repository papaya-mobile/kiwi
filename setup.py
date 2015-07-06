# -*- coding: utf-8 -*-

from setuptools import setup

version = '0.0.1'

setup(name='kiwi',
      version=version,
      packages=['kiwi'],
      description='Simple dynamodb ORM',
      author='Papaya Backend',
      author_email='backend@papayamobile.com',
      install_requires=['future', 'boto>=2.38.0'],
      )
