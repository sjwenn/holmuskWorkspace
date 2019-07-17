'''
Hello! This is the JWComorbid collection of modules. 

They are created to replicate a study titled 
"Comorbid substance use disorders with other Axis I and II mental disorders
among treatment-seeking Asian Americans, Native Hawaiians/Pacific Islanders, and mixed-race people".
`Here is the paper. <https://doi.org/10.1016/J.JPSYCHIRES.2013.08.022>`_

JWComorbid consists of 7 modules, structured as follows:

.. image:: https://drive.google.com/uc?export=view&id=1wrNcFnupN_zXXtRtYhcUNuOtu5rK5p0o

Each module falls into one of these layers:

.. image:: https://drive.google.com/uc?export=view&id=1s75VDvlbMXj48eA2Y6jc7LdyJxhpQOeU


Fetch and Preprocess Layer
------
**preprocessDB.py**

Runs SQL commands to work with database tables to generate a master table which has all needed information. 
Table will then be fetched and saved as data/intermediate/db.pickle .

Data Extraction Layer
------
**getUsefulInfo.py**

Retrieves useful/commonly used information such as metrics and category lists from the master table.
Loads the master table from db.pickle and stores useful information in a dict called "data".
Data will then be saved in 	data/intermediate/data.pickle .


Results Layer
------
**table1.py**

Generates Table 1, which is the overview of characteristics with respect to each race.

**figure1.py**

Generates Figure 1, which is a bar plot of the percentage of patients of a certain race presenting with a certain mental diagnosis 
compared to the total number of patients in said race.

**table2.py**

Generates Table 2, which displays substance use disorder (SUD) diagnoses among patients aged 12 or older, by age group.

**table3.py**

Generates Table 3, which displays the adjusted odds ratios of substance use disorder (SUD) diagnoses among people aged 12 years or older.

**table4.py**

Generates Table 4, which displays the adjusted odds ratios of having a comorbid SUD diagnosis in relation to other psychiatric diagnoses among people aged 12 years or older.


Before you Begin
================

If you get this error:

.. code-block:: none

	Attribute Qt::AA_EnableHighDpiScaling must be set before QCoreApplication is created.

from figure1.py, don't sweat it. I don't think it affects the plotting of Figure 1, only
limits the DPI. I'm not too sure how to fix it but it happens for some environments.


Specifications:
===============

You should need to configure your own db.json for this to work.
Also, in preProcessDB.json, you need to specify your own filters for:

.. code-block:: python
    
	"raceFilterPath"    : "<your filter (Race)>", 
	"settingFilterPath" : "<your filter (Setting)>", 
	"sexFilterPath"     : "<your filter (Sex)>", 
	"dsmSUDPath"        : "<your filter (SUD)>", 
	"dsmDiagnosesPath"  : "<your filter (Diagnoses)>", 

An example sex filter can be founnd `here. <https://drive.google.com/open?id=1ZDMyZPgKf8Ty9B3GIFU-EVrjccQ3qEdQ>`_
The filter format is the same for setting and race.

Filters for SUD and Diagnoses are `here <https://drive.google.com/open?id=11YzrdKk9_SEmV1MK4X8enaBLjRxRoPKv>`_ and `here <https://drive.google.com/open?id=1jYMj3NKb50D8pagBxfQuCVg6B4IBEz2d>`_ respecitvely.


Do specify a workspace schema and final table name.  

.. code-block:: python
    
	"schemaName" : "<your schema>",
	"tableName"  : "<your table>"

Its gonna create temp tables

.. code-block:: python
    
	<your schema>.temp1 
	<your schema>.temp2
	<your schema>.temp3
	<your schema>.temp4
	
So ensure that your schema does not have any of these tables already, else it will conflict.


Specifications for ``modules.json``
-----------------------------------

Make sure that the ``execute`` statement within the modules file for each module you want activated is set to True. 

For example:

.. code-block:: python
    :emphasize-lines: 3

	"moduleName" : "preProcessDB",
	"path"       : "modules/JWComorbid/preProcessDB/preProcessDB.py",
	"execute"    : true,
	"description": "",
	"owner"      : ""
	
There are a bunch like the above; one for every module in JWComorbid. Set those to "true" too.


Results
=======

The tables should be printed in stdout.
In addition, Figure 1 should be saved to /manuscript/figure1.png

'''
