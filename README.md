[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=wuan_bo-python&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=wuan_bo-python)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=wuan_bo-python&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=wuan_bo-python)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=wuan_bo-python&metric=coverage)](https://sonarcloud.io/summary/new_code?id=wuan_bo-python)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=wuan_bo-python&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=wuan_bo-python)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/wuan/bo-python/badge)](https://scorecard.dev/viewer/?uri=github.com/wuan/bo-python)

python-blitzortung a python module for blitzortung.org related stuff
--------------------------------------------------------------------

# Introduction

This module contains the following independent components

* strike and station data import from data.blitzortung.org
* database setup and access for local storage of blitzortung.org data.
* clustering of strike data

Please have a look at https://github.com/wuan/bo-server for related scripts/cronjobs

# Installation

## Install pip

> wget https://bootstrap.pypa.io/get-pip.py
> sudo pypy3 get-pip.py
Install manually by entering

## Install library with dependencies

> sudo pypy3 -m pip install -e .

or build a debian package by entering

> dpkg-buildpackage

The following software has to be installed manually, depending on their availability as debian/ubuntu packages.

Scipy and fastcluster is required for the (optional) clustering
