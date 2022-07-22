#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

setup(
    name='blitzortung',
    version='1.7.1',
    description='blitzortung.org python modules',
    long_description="""a library providing python classes for blitzortung operation""",
    author='Andreas Wuerl',
    author_email='blitzortung@tryb.de',
    url='https://github.com/wuan/bo-python',
    license='Apache 2',
    packages=find_packages(),
    install_requires=[
        'backports.zoneinfo',
        'injector',
        'requests',
        'lockfile',
        'shapely',
        'pyproj==3.2.1',
        'twisted',
        'psycopg2'
    ],
    extras_require={
        'testing': [
            'pytest-cov',
            'pytest-benchmark',
            'mock',
            'assertpy'
        ],
    },
    platforms='OS Independent',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ],
)
