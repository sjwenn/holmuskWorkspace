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
import scipy
from scipy.stats import chi2
from scipy.stats import chi2_contingency
import pickle
import math
import re
from tabulate import tabulate
import pandas as pd
import time
from lib.databaseIO import pgIO
from multipledispatch import dispatch
import numpy.ma as ma

from modules.RWEWidgets.rwe import Core as rwe

config     = jsonref.load(open('../config/config.json'))

jsonConfig = jsonref.load(open('../config/modules/RWEWidgets/widgets.json'))

logBase    = config['logging']['logBase'] + '.modules.RWEWidgets.widgets'
dbName     = jsonConfig["inputs"]["dbName"]

rawSchemaName = jsonConfig["inputs"]["rawSchemaName"]

def Chi2(cohort, input1, input2):

    cohortColumn = cohort.name
    input1Column = input1.name
    input2Column = input2.name

    cohortData = cohort.df
    input1Data = input1.df
    input2Data = input2.df

    if (cohort.hasTime or input1.hasTime or input2.hasTime):
        raise Exception('[Error] Chi2: Inputs must be time independent')
        return

    cohortData.set_index('cohort', inplace=True)
    input1Data.set_index('cohort', inplace=True)
    input2Data.set_index('cohort', inplace=True)


    input1Data = input1Data.loc[cohortData[cohortColumn] == True]
    input2Data = input2Data.loc[cohortData[cohortColumn] == True]

    input1Data['cat'] = (input1Data.iloc[:, 1:] == 1).idxmax(1)
    input2Data['cat'] = (input2Data.iloc[:, 1:] == 1).idxmax(1)

    return [input1Data, input2Data]