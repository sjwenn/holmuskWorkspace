# JWComobid Collection of Modules

> **TL;DR** 
To activate me, modify `/config/modules.json` by making ```"execute": true``` in my module.
i.e.
```
"moduleName" : "JWComorbid",
"path"       : "modules/JWComorbid/JWComorbid.py",
"execute"    : true,
"description": "Collection of modules",
"owner" : ""
```
>To toggle individual modules, modify `/config/modules/JWComorbid/modules.json`.

>A "run all" override can be found in `/config/modules/JWComorbid/JWComorbid.json`.
It will ignore whatever is in `/config/modules/JWComorbid/modules.json` and run everything.

### Hello! This is the JWComorbid collection of modules. 

They are created to replicate a study titled 
"Comorbid substance use disorders with other Axis I and II mental disorders
among treatment-seeking Asian Americans, Native Hawaiians/Pacific Islanders, and mixed-race people".

[Here is the paper.](https://doi.org/10.1016/J.JPSYCHIRES.2013.08.022)

JWComorbid consists of 7 modules, structured as follows:

![graph](https://drive.google.com/uc?export=view&id=1wrNcFnupN_zXXtRtYhcUNuOtu5rK5p0o)

Each module falls into one of these layers:

![pyramid](https://drive.google.com/uc?export=view&id=1s75VDvlbMXj48eA2Y6jc7LdyJxhpQOeU)


## Fetch and Preprocess Layer
#### preprocessDB.py

Runs SQL commands to work with database tables to generate a master table which has all needed information. 
Table will then be fetched and saved as data/intermediate/db.pickle .

## Data Extraction Layer
#### getUsefulInfo.py

Retrieves useful/commonly used information such as metrics and category lists from the master table.
Loads the master table from db.pickle and stores useful information in a dict called "data".
Data will then be saved in 	data/intermediate/data.pickle .


## Results Layer
#### table1.py

Generates Table 1, which is the overview of characteristics with respect to each race.

#### figure1.py

Generates Figure 1, which is a bar plot of the percentage of patients of a certain race presenting with a certain mental diagnosis 
compared to the total number of patients in said race.

#### table2.py

Generates Table 2, which displays substance use disorder (SUD) diagnoses among patients aged 12 or older, by age group.

#### table3.py

Generates Table 3, which displays the adjusted odds ratios of substance use disorder (SUD) diagnoses among people aged 12 years or older.

#### table4.py

Generates Table 4, which displays the adjusted odds ratios of having a comorbid SUD diagnosis in relation to other psychiatric diagnoses among people aged 12 years or older.
