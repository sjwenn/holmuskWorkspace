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
#from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table1.json'))
logBase = config['logging']['logBase'] + '.modules.table1.table1'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    print('='*40 + "\n" + "Table 1")

    for race in np.append('', data['list race']):
    # '' represents 'Total'.
        print('='*40)

        if race == '':
            print("Total", end="")
        print(race, end="")

        print(" (" + str(data['count '+race]) + ")")

        print("\nAge in years")
        for age in data['list age']:
            print(age.ljust(9) + ": " + str(data['count '+race+age]))

        print("\nSex")
        for sex in data['list sex']:
            print(sex.ljust(9) + ": " + str(data['count '+race+sex]))   

    print('='*40)

    return




