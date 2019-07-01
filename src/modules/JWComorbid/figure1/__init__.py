'''Generates Figure 1, which is a bar plot of the percentage of patients of a certain race presenting with a certain mental diagnosis 
compared to the total number of patients in said race.

Before you Begin
================

If you get this error:

.. code-block:: none

	Attribute Qt::AA_EnableHighDpiScaling must be set before QCoreApplication is created.

from figure1.py, don't sweat it. I don't think it affects the plotting of Figure 1, only
limits the DPI. I'm not too sure how to fix it but it happens for some environments.


Details of Operation
====================

[
Over here, you should provide as much information as possible for what the modules does. 
You should mention the data sources that the module uses, and important operations that
the module performs.
]

Results
=======

Expected results are printed to ``saveFigPath`` as defined in ``figure1.json``.

An example result would look like this:

.. code-block:: none

	========================================
	Figure 1
	========================================
	Saved to ../manuscript/figure1.png

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

    "moduleName" : "figure1",
    "path"       : "modules/JWComorbid/figure1/figure1.py",
    "execute"    : true,
    "description": "",
    "owner"      : ""


'''
