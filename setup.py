#!/usr/bin/env python
""" SqlAlchemy N+1 Loader: a solution to the N+1 problem """

from setuptools import setup, find_packages

setup(
    name='nplus1loader',
    version='1.0.0',
    author='Mark Vartanyan',
    author_email='kolypto@gmail.com',

    url='https://github.com/kolypto/py-nplus1loader',
    license='BSD',
    description=__doc__,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    keywords=['sqlalchemy', 'nplus1'],

    packages=find_packages(exclude=('tests',)),
    scripts=[],
    entry_points={},

    python_requires='>= 3.6',
    install_requires=[
        'sqlalchemy >= 1.3.0',
        'funcy',
    ],
    extras_require={},
    include_package_data=True,
    test_suite='nose.collector',

    platforms='any',
    classifiers=[
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Programming Language :: Python :: 3',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
)
