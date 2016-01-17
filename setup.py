# -*- coding: utf-8 -*-

from setuptools import setup

options = {
  'name': 'python-dbase',
  'version': '0.1',
  'description': 'Python dbase operations module',
  'url': 'https://github.com/python-dbase/python-dbase',
  'author': 'David Kwast',
  'author_email': 'david@kwast.net',
  'license': 'MIT',
  'packages': ['dbase'],
  'zip_safe': False  
}

setup(**options)