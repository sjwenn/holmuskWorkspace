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


config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/getUsefulInfo.json'))
logBase = config['logging']['logBase'] + '.modules.getUsefulInfo.getUsefulInfo'

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    dbName = jsonConfig["inputs"]["dbName"]

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"db.pickle",'rb') 
    (miscData, rawData) = pickle.load(fileObjectLoad)  
    SUDList          = miscData[0]
    diagnosesList    = miscData[1]
    rawSUDList       = miscData[2]
    rawdiagnosesList = miscData[3]  
    fileObjectLoad.close()

    rawData = rawData[rawData['age']!='0']

    raceList = rawData['race'].unique()
    ageList  = rawData['age_categorical'].unique()
    sexList  = rawData['sex'].unique()

    rawData[SUDList]       = rawData[SUDList].mask(rawData[SUDList]>0, 1)
    rawData[diagnosesList] = rawData[diagnosesList].mask(rawData[diagnosesList]>0, 1)
    rawData['SUD Count']   = rawData[SUDList].apply(lambda x: x.sum(), axis=1)

    rawData['Any SUD'] = 0
    rawData.loc[rawData['SUD Count'] >= 1, 'Any SUD'] = 1

    rawData['>=2 SUDs'] = 0
    rawData.loc[rawData['SUD Count'] >= 2, '>=2 SUDs'] = 1

    data =  {}
    data["df"] = rawData

    for race in raceList:
        inrace = rawData[rawData['race']==race]
        data["count "+race] = len(inrace)

        for age in ageList:
            data["count " + race + age] = len(inrace[inrace['age_categorical']==age])

        for sex in sexList:
            data["count " + race + sex] = len(inrace[inrace['sex']==sex])

        for SUD in SUDList:
            data["count " + race + SUD] = len(inrace[inrace[SUD]==1])
           
        for diagnoses in diagnosesList:
            data["count " + race + diagnoses] = len(inrace[inrace[diagnoses]==1]) 


    for age in ageList:
        data["count "+age] = len(rawData[rawData['age_categorical']==age])

    for sex in sexList:
        data["count "+sex] = len(rawData[rawData['sex']==sex])

    for SUD in SUDList:
        data["count " + SUD] = len(rawData[rawData[SUD]==1])

    for diagnoses in diagnosesList:
        data["count " + diagnoses] = len(rawData[rawData[diagnoses]==1]) 

    data["count everyone"] = len(rawData[rawData['SUD Count']>=0])
    data["count Any SUD"] = len(rawData[rawData['SUD Count']>=1])
    data["count >=2 SUDs"] = len(rawData[rawData['SUD Count']>=2])

    data["list race"]      = raceList
    data["list age"]       = ageList
    data["list sex"]       = sexList
    data["list SUD"]       = SUDList
    data["list diagnoses"] = diagnosesList

    data["count "] = data["count everyone"] # For convinience

    fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"data.pickle",'wb') 
    pickle.dump(data, fileObjectSave, protocol=pickle.HIGHEST_PROTOCOL)   
    fileObjectSave.close()


    # NOTE THAT 'ANY SUD' WILL BE TAKEN FROM 'substance_abuse' COLUMN. 
    # THIS IS MORE GENERAL AND INCLUDES 'GENERAL SUBSTANCE ABUSE'.

    return




