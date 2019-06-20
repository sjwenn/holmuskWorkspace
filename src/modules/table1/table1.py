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

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"db.pickle",'rb') 
    rawData = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    print(rawData.head(10))

    return




