from logs import logDecorator as lD 
import jsonref, pprint
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
sns.set(style="dark")
sns.set_palette(sns.diverging_palette(240, 120, l=60, n=3, center="dark"))
from scipy import stats
from scipy.stats import chi2
from scipy.stats import chi2_contingency
import pickle
import math
import re
from tabulate import tabulate

import dask.array as da
import dask.dataframe as dd
import pandas as pd

import time

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table3.json'))
logBase = config['logging']['logBase'] + '.modules.table3.table3'

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    dbName = jsonConfig["inputs"]["dbName"]
    tableNameComorbid = jsonConfig["inputs"]["tableNameComorbid"]
    tableNameDiagnoses = jsonConfig["inputs"]["tableNameDiagnoses"]

    fetchQuery = "select * from " + tableNameDiagnoses + ";"
    genRetrieve = pgIO.getDataIterator( fetchQuery, dbName = dbName, chunks = 1000)
    tempArray = []
    for idx, data in enumerate(genRetrieve):
    	tempArray.append(data)
    	print("Chunk: "+str(idx))

    dsmSUD          = pd.read_csv(jsonConfig["inputs"]["dsmSUDPath"])
    dsmDiagnoses    = pd.read_csv(jsonConfig["inputs"]["dsmDiagnosesPath"])

    SUDList         = dsmSUD.columns.tolist()
    DiagnosesList   = dsmSUD.columns.tolist()

    cols = ['id','siteid','race','sex','age_numeric','visit_type','age', 'dsm', 'diagnosis']

    df = pd.DataFrame(data = tempArray[0], columns = cols)

    # for item in SUDList:
    #     df[item]=0
    # for item in DiagnosesList:
    #     df[item]=0

    # for column in dsmSUD:
    #     for row in dsmSUD[column]:
    #         df.replace(row, column, inplace=True)

    # for column in dsmDiagnoses:
    #     for row in dsmDiagnoses[column]:
    #         df.replace(row, column, inplace=True)

    

    print(df.head(100))

    return




