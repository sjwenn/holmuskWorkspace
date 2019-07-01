'''Generates Table 3, which displays the adjusted odds ratios of substance use disorder (SUD) diagnoses among people aged 12 years or older.

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

An example result would look like `this. <https://drive.google.com/open?id=1hn1gnImM8CrxLjvXGopHBgJy23JytRrG>`_

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

    "moduleName" : "table3",
    "path"       : "modules/JWComorbid/table3/table3.py",
    "execute"    : true,
    "description": "",
    "owner"      : ""

'''
