#!/usr/bin/env python

from setuptools import setup, find_packages

import blitzortung

setup(
    name='blitzortung',
    packages=find_packages(),
    install_requires=['injector', 'pytz', 'dateutils', 'shapely', 'pyproj', 'statsd'],
    tests_require=['nose', 'mock', 'coverage', 'assertpy'],
    version=blitzortung.__version__,
    description='blitzortung.org python modules',
    download_url='http://www.tryb.de/andi/blitzortung/',
    author='Andreas Wuerl',
    author_email='blitzortung@tryb.de',
    url='http://www.blitzortung.org/',
    license='GPL-3 License',
    long_description="""a library providing python classes for blitzortung operation""",
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
