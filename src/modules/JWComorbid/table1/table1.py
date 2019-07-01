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
jsonConfig = jsonref.load(open('../config/modules/JWComorbid/table1.json'))
logBase = config['logging']['logBase'] + '.modules.JWComorbid.table1'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.CI')
def CI(logger, p, n, CL):
    '''Confidence Interval
    
    Calculates confidence interval amplitude.

    How to use?

    Lower bound = Probability + CI

    Upper bound = Probability - CI
    
    Arguments:
        p {[float]} -- Probability
        n {[int]} -- Population size
        CL {[float]} -- Confidence level
    
    Returns:
        [float] -- Confidence Interval (CI)

    '''
    SE = math.sqrt(p*(1-p)/n)  
    z_star = stats.norm.ppf((1-CL)/2)
    ME = z_star * SE

    return ME

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    # Load dictionary from pickle
    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    print('='*40)
    print("Table 1")

    for race in np.append('', data['list race']): # '' represents total
    
        print('='*40)

        raceCount = data['count '+race]

        # race == '' means accounting for total population. Note type of data collected is different from individual races i.e. (N,%) vs (%,CI)
        if race == '':
            print("Total (N = {})".format(raceCount), end='')

            print("\nAge in years")

            for age in data['list age']:
                count = data['count '+race+age]
                proportion = count / raceCount
                print( "{}: {} ({})".format(age.ljust(9), str(count).ljust(8), round(proportion*100,1) ))

            print("\nSex")

            for sex in data['list sex']:
                count = data['count '+race+sex]
                proportion = count / raceCount
                print( "{}: {} ({})".format(sex.ljust(9), str(count).ljust(8), round(proportion*100,1) ))

        else:
            print("{} (N = {})".format(race, raceCount))

            print("\nAge in years")

            for age in data['list age']:
                proportion = data['count '+race+age] / raceCount
                confidenceInterval = CI(proportion, raceCount, 0.95)
                lowerBound = (proportion + confidenceInterval) * 100
                upperBound = (proportion - confidenceInterval) * 100
                print( "{}: {} ({}-{})".format(age.ljust(9), str(round(proportion*100,1)).ljust(8), round(lowerBound,1), round(upperBound,1) ))

            print("\nSex")

            for sex in data['list sex']:
                proportion = data['count '+race+sex] / raceCount
                confidenceInterval = CI(proportion, raceCount, 0.95)
                lowerBound = (proportion + confidenceInterval) * 100
                upperBound = (proportion - confidenceInterval) * 100
                print( "{}: {} ({}-{})".format(sex.ljust(9), str(round(proportion*100,1)).ljust(8), round(lowerBound,1), round(upperBound,1) ))

    return




