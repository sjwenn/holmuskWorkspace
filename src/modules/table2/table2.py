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
import statsmodels.formula.api as sm

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table2.json'))
logBase = config['logging']['logBase'] + '.modules.table2.table2'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    
    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    df = data['df']
    print("Table 2")

    for race in data['list race']:

        print('='*40)

        inRace = df[df['race']==race]

        for age in np.append('', data['list age']):
            if age != '1-11':
                print('')
                if age == '':
                    print( (race + " Total").ljust(26) + " (", end="")
                    inRaceAge = inRace
                else:
                    print( (race + " " + age).ljust(26) + " (", end="")
                    inRaceAge = inRace[inRace['age_categorical']==age]

                countRaceAge = data['count '+race+age]

                print( str(countRaceAge) + ")")

                countRaceAgeSUD = len(inRaceAge[inRaceAge['SUD Count'] >= 1])/countRaceAge
                print("Any SUD".ljust(28) + str(  round( countRaceAgeSUD*100 ,1 )   ))

                countRaceAgeSUD = len(inRaceAge[inRaceAge['SUD Count'] >= 2])/countRaceAge
                print(">=2 SUDs".ljust(28) + str(  round( countRaceAgeSUD*100 ,1 )   ))

                for SUD in data['list SUD']:
                    countRaceAgeSUD = len(inRaceAge[inRaceAge[SUD] == 1])/countRaceAge
                    print(SUD.ljust(28) + str(  round( countRaceAgeSUD*100 ,1 )   ))



    return




