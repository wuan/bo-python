#!/usr/bin/env python

from distutils.core import setup

import blitzortung

setup(
    name            = 'blitzortung',
    version         = blitzortung.__version__,
    description     = 'blitzortung.org python modules',
    download_url    = 'http://www.tryb.de/andi/blitzortung/',
    author          = 'Andreas Wuerl',
    author_email    = 'blitzortung@tryb.de',
    url             = 'http://www.blitzortung.org/',
    packages        = ['blitzortung'],
    scripts	    = [],
    license         = 'GPL-3 License',
    long_description = """a library providing python classes for blitzortung operation""",
    platforms = 'OS Independent',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GPL-3 License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Education :: Testing',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ],
)
