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

import dask.array as da
import dask.dataframe as dd
import pandas as pd

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/readmeGenerator.json'))
logBase = config['logging']['logBase'] + '.modules.readmeGenerator.readmeGenerator'

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''main function for module1
    
    This function reads data from a SQL server, saves
    into a pickle and loads it before finally doing a
    length integrity check between the two. Just some 
    practice; not very useful at all.
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''
    try: #table 1
        fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"table1String.pickle",'rb') 
        table1String = pickle.load(fileObjectLoad)   
        fileObjectLoad.close()

    except Exception as e:
        logger.error(f'Issue with Table 1: " {e}')


    try: #figure 1
        with open(jsonConfig["outputs"]["reportPath"] + 'report.md', 'w') as f:
            f.write( table1String + "\n# Figure 1 \n![image](figure1.png)\n" )

    except Exception as e:
        logger.error(f'Issue with Figure 1: " {e}')


    return




