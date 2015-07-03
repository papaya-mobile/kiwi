# -*- coding: utf-8 -*-

from setuptools import setup


setup(name='kiwi',
      version='0.0.1',
      packages=['kiwi'],
      description='Simple dynamodb ORM',
      author='Papaya Backend',
      author_email='backend@papayamobile.com',
      install_requires = ['boto>=2.38.0'],
      )
