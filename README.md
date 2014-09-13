python-blitzortung a python module for blitzortung.org related stuff
--------------------------------------------------------------------

Current build status: [![Build Status](https://travis-ci.org/wuan/bo-python.svg?branch=master)](https://travis-ci.org/wuan/bo-python)

# Introduction

This modules contains the following independent components

* strike and station data import from data.blitzortung.org
* database setup and access for local storage of blitzortung.org data.
* clustering of strike data

Please have a look at https://github.com/wuan/bo-server for related scripts/cronjobs

# Installation

Install manually by entering

> python setup.py install

or build a debian package by entering

> dpkg-buildpackage

The following software has to be installed manually, depending on their availability as debian/ubuntu packages.

Requires numpy >= 1.7 and pandas >= 0.9 to have a finally usable 64 bit timestamp support.

Scipy and fastcluster is required for the clustering

Requires injector.

 -- Andreas Würl <blitzortung@tryb.de>  Sat, 13 Sep 2014 17:11:00 +0200
