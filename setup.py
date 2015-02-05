#!/usr/bin/env python
from setuptools import setup, find_packages
import os
import sys
import XenCert

def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    f = open(path)
    return f.read()

install_requires = []
pyversion = sys.version_info[:2]
if pyversion < (2, 7) or (3, 0) <= pyversion <= (3, 1):
    install_requires.append('argparse')


setup(
    name='inspur-xencert',
    version=XenCert.__version__,
    description='The Inspur Xen Storage Certification Kit',
    long_description=read('README.md'),
    author='Zhao Zhenlong',
    author_email='zhaozhl@inspur.com',
    url='https://github.com/lielongxingkong/xencert',
    packages=find_packages(),
    install_requires=[
        'setuptools'
    ] + install_requires,
    license='LGPL',
)
