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

import statsmodels.api as sm

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table3.json'))
logBase = config['logging']['logBase'] + '.modules.table3.table3'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.logisticRegression')
def logisticRegression(logger, df):

    return

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    df = data['df']

    df.to_csv('test.csv')

    print("Table 3")

    dfModified = df[df['sex']!='Others']

    dfModified['intercept'] = 1

    parameters = ['race', 'age_categorical', 'sex']

    endog = dfModified['Any SUD']
    exog = pd.get_dummies(dfModified[parameters])
    exog.drop('race_AA', axis=1, inplace=True)
    exog.drop('sex_M', axis=1, inplace=True)
    exog.drop('age_categorical_50+', axis=1, inplace=True)
    result = sm.Logit(endog, exog).fit()
    print(result.summary())
    config = result.conf_int()
    print(np.exp(result.params))

    endog = dfModified['>=2 SUDs']
    exog = pd.get_dummies(dfModified[parameters])
    exog.drop('race_AA', axis=1, inplace=True)
    exog.drop('sex_M', axis=1, inplace=True)
    exog.drop('age_categorical_50+', axis=1, inplace=True)
    result = sm.Logit(endog, exog).fit()
    print(result.summary())
    config = result.conf_int()
    print(np.exp(result.params))

    parameters = ['race']

    for race in data['list race']:
        inRace = dfModified[dfModified['race']==race]
        endog = inRace['Any SUD']
        exog = inRace[data['list SUD']]
        result = sm.Logit(endog, exog).fit()
        print(result.summary())
        config = result.conf_int()
        print(np.exp(result.params))

    return



























# train_cols = df.columns[1:]
# logit = sm.Logit(df['sud'], df[train_cols])
# result = logit.fit()

# params = result.params
# config = result.conf_int()
# conf['OR'] = params

# conf.columns = ['2.5%', '97.5%', 'OR']
# CI_OR_df = np.exp(conf)
# resultsDF = CI_OR_df[['OR']].join(CI_OR_df.ix[:,:'97.5%'])


