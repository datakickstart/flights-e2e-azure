"""
Setup script for datakickstart_dabs.

This script packages and distributes the associated wheel file(s).
Source code is in ./src/. Run 'python setup.py sdist bdist_wheel' to build.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import flights

setup(
    name="flights",
    version=flights.__version__,
    url="https://databricks.com",
    author="training@dustinvannoy.com",
    description="Flights package",
    packages=find_packages(where='.'),
    package_dir={'': 'src'},
    install_requires=["setuptools"],
)
