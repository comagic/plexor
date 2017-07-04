#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(name='plexor_test',
      version='1.0',
      entry_points={
          'console_scripts': [
              'run = scripts.run:main'
          ]
      },
      packages=find_packages())
