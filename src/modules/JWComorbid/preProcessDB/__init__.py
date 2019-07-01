'''Runs SQL commands to work with database tables to generate a master table which has all needed information. 

The table will then be fetched and saved as data/intermediate/db.pickle . 
This is a very heavy module and is going to be replaced after the SQL-python-pgIO-psycopg2 wrapper thing is done.

Before you Begin
================

Make sure that the configuration files are properly set, as mentioned in the Specifcations 
section.

Details of Operation
====================

[
Over here, you should provide as much information as possible for what the modules does. 
You should mention the data sources that the module uses, and important operations that
the module performs.
]

Results
=======

The expected output would be a pickle being created at "data/intermediate/db.pickle".

Specifications:
===============

Specifications for running the module is described below. Note that all the json files
unless otherwise specified will be placed in the folder ``config`` in the main project
folder.

Specifications for ``modules.json``
-----------------------------------

Make sure that the ``execute`` statement within the modules file is set to True. 

.. code-block:: python
    :emphasize-lines: 3

    "moduleName" : "preProcessDB",
    "path"       : "modules/JWComorbid/preProcessDB/preProcessDB.py",
    "execute"    : true,
    "description": "",
    "owner"      : ""

'''

