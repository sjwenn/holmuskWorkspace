'''Generates Table 2, which displays substance use disorder (SUD) diagnoses among patients aged 12 or older, by age group.

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

Expected results are printed to stdout.

An example result would look like `this. <https://drive.google.com/open?id=1F0Mm8VryTmiy3ZO2hUL9erO_qZYoJCFd>`_

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

    "moduleName" : "table2",
    "path"       : "modules/JWComorbid/table2/table2.py",
    "execute"    : true,
    "description": "",
    "owner"      : ""



'''
