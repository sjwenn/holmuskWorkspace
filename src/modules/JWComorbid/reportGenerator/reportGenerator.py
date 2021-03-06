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
import pandas as pd
import time
from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/reportGenerator.json'))
logBase = config['logging']['logBase'] + '.modules.JWComorbid.reportGenerator'
dbName     = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    
    return




