#!/usr/bin/env python

import sys

from setuptools import setup, find_packages

if not sys.version_info[0] == 3:
    print('only python3 supported!')
    sys.exit(1)

setup(
    name='gdprSHARK',
    version='0.0.1',
    author='Steffen Tunkel',
    author_email='steffen.tunkel',
    description='',
    install_requires=['mongoengine', 'pymongo', 'pycoshark>=1.0.3', 'pandas', 'numpy'],
    dependency_links=['git+https://github.com/smartshark/pycoSHARK.git@1.0.3#egg=pycoshark-1.0.3'],
    url='https://github.com/smartshark/gdprSHARK',
    download_url='https://github.com/smartshark/gdprSHARK/zipball/master',
    packages=find_packages(),
    test_suite ='tests',
    zip_safe=False,
    include_package_data=True,
    classifiers=[
    ],
)
