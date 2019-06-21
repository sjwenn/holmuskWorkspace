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
jsonConfig = jsonref.load(open('../config/modules/figure1.json'))
logBase = config['logging']['logBase'] + '.modules.figure1.figure1'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    df = data['df']

    dfFigure1Buffer = []

    for race in data["list race"]:
        inrace = df[df['race']==race]
        raceCount = data['count '+race]
        for diagnosis in data["list diagnoses"]:
            percentage = len(inrace[inrace[diagnosis]==1])/raceCount*100
            dfFigure1Buffer.append([ percentage, diagnosis, race] )

    dfFigure1 = pd.DataFrame(dfFigure1Buffer, columns=['%', 'Diagnosis', 'Race'])

    # Remove fields less than 3
    for diagnosis in data["list diagnoses"]:
        morethan3Buffer = dfFigure1[dfFigure1['Diagnosis']==diagnosis]
        if (morethan3Buffer['%'] < 3).all():
            dfFigure1 = dfFigure1.drop(morethan3Buffer.index)

    plt.figure(figsize=(15,8)) 
    ax = sns.barplot(x="Diagnosis", y="%", hue="Race", data=dfFigure1, hue_order=['AA', 'NHPI', 'MR'])
    plt.xticks(rotation=45)

    #Put numerical labels above every bar
    for p in ax.patches: 
        height = p.get_height()
        ax.text(p.get_x()+p.get_width()/2.,
                height + 0.5,
                '{:1.0f}'.format(height),
                ha="center") 

    plt.savefig(jsonConfig["outputs"]["saveFigPath"], dpi=600, bbox_inches = "tight")
    plt.show()



    return




