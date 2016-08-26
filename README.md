qsqla
===============================

[![Build Status](https://travis-ci.org/blue-yonder/qsqla.svg?branch=master)](https://travis-ci.org/blue-yonder/qsqla)

qSQLA is a query builder for SQLAlchemy Core Selectables 

Installation / Usage
--------------------

To install use pip:

    $ pip install qsqla


Or clone the repo:

    $ git clone https://github.com/blue-yonder/qsqla.git
    $ python setup.py install

Example
-------


```python

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String


db = create_engine("sqlite:////:memory:")
md = MetaData(bind=db)

table = Table('user', 
              md,
              Column('id', Integer, primary_key=True),
              Column('name', String(16), nullable=False),
              Column('age', Integer)
              )

sel = table.select()

from qsqla.query import query, build_filters
filter =  build_filters({"id__eq":1})
stm = query(sel, filter)

```
