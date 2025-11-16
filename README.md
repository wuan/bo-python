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

It is recommended to install the package in a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install blitzortung
```

For development installation from source:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

## Optional Dependencies

Scipy and fastcluster are required for the (optional) clustering functionality. These can be installed via:

```bash
pip install scipy fastcluster
```
