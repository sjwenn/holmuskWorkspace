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
pd.options.mode.chained_assignment = None
import time

import statsmodels.api as sm

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table3.json'))
logBase = config['logging']['logBase'] + '.modules.table3.table3'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    df = data['df']

    print('='*40)
    print("Table 3")

    dfModified = df
    dfModified = dfModified[dfModified['sex']!='Others']
    dfModified = dfModified[dfModified['age_categorical']!='1-11']



    for race in np.append('', data['list race']):

        if race != '':
            inRace                    = dfModified[dfModified['race']==race]
            raceLabel                 = race
            parameters                = ['age_categorical', 'sex']
            exog                      = pd.get_dummies(inRace[parameters])

        else:
            inRace                    = dfModified
            raceLabel                 = "Total"
            parameters                = ['race', 'age_categorical', 'sex']
            exog                      = pd.get_dummies(inRace[parameters])
            exog.drop('race_AA', axis = 1, inplace=True)

        exog['intercept'] = 1
        exog.drop('sex_F', axis=1, inplace=True)
        exog.drop('age_categorical_50+', axis=1, inplace=True)

        for item in ['Any SUD', '>=2 SUDs']:

            print('='*40 + "\n" + item + " " + raceLabel)

            endog = inRace[item]

            result = sm.Logit(endog, exog).fit()

            relavantResults         = result.conf_int(alpha=0.05)
            relavantResults['OR']   = result.params
            relavantResults.columns = ['5%', '95%', 'OR']
            relavantResults         = relavantResults[['OR', '5%', '95%']]

            oddsRatio = np.exp(relavantResults)

            print(oddsRatio)

    return




























