'''Retrieves commonly used/useful data, such as but not limited to lists and metrics, from the master database table.

Loads data from db.pickle, creates a dict and stores useful data along with the original database in it.
Finally, saves the dict as data.pickle.

The save location for these pickles are specified in the JSON, under inputs and outputs for db.pickle and data.pickle, respectively.

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

The expected output would be a pickle being created at "data/intermediate/data.pickle".

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

    "moduleName" : "getUsefulInfo",
    "path"       : "modules/JWComorbid/getUsefulInfo/getUsefulInfo.py",
    "execute"    : true,
    "description": "",
    "owner"      : ""

'''
