python-blitzortung a python module for blitzortung.org related stuff
--------------------------------------------------------------------

[![Build Status](https://travis-ci.org/wuan/bo-python.svg?branch=master)](https://travis-ci.org/wuan/bo-python)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/ee40a9ada1a14cc7b061a0080bdc4d84)](https://www.codacy.com/gh/wuan/bo-python/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=wuan/bo-python&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/ee40a9ada1a14cc7b061a0080bdc4d84)](https://www.codacy.com/gh/wuan/bo-python/dashboard?utm_source=github.com&utm_medium=referral&utm_content=wuan/bo-python&utm_campaign=Badge_Coverage)

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

