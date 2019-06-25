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

    # Exclude specified values. This does not go into the logit. Specified in JSON.
    for [subject, value] in jsonConfig["params"]["toExclude"]:
        dfModified = dfModified[dfModified[subject]!=value]


    for race in np.append('', data['list race']):

        print('='*40)

        if race != '':
            inRace                    = dfModified[dfModified['race']==race]
            raceLabel                 = race
            parameters                = jsonConfig["params"]["logitParameters"]
            exog                      = pd.get_dummies(inRace[parameters])

        else:
            inRace                    = dfModified
            raceLabel                 = "Total"
            parameters                = ['race'] + jsonConfig["params"]["logitParameters"]
            exog                      = pd.get_dummies(inRace[parameters])
            exog.drop('race_AA', axis = 1, inplace=True)

        exog['intercept'] = 1

        # Drop specified values. Specified in JSON.
        for toDrop in jsonConfig["params"]["toDropExog"]:
            exog.drop(toDrop, axis=1, inplace=True)

        # Multiple sets of regressions can be run. Specified in JSON.
        for item in jsonConfig["params"]["targetVariables"]:

            print( "\n" + item + " " + raceLabel)

            endog = inRace[item]

            result = sm.Logit(endog, exog).fit(disp=0)

            # Get confidence interval and order data
            relavantResults         = result.conf_int(alpha=0.05)
            relavantResults['OR']   = result.params
            relavantResults.columns = ['5%', '95%', 'OR']
            relavantResults         = relavantResults[['OR', '5%', '95%']]

            # Get odds ratio from logistic regression coefficients
            oddsRatio = np.exp(relavantResults)

            oddsRatio = np.round(oddsRatio, 2)

            print(oddsRatio)

    return




























