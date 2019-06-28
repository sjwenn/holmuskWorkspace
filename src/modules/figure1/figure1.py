from logs import logDecorator as lD 
import jsonref, pprint
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
#sns.set_palette(sns.diverging_palette(240, 120, l=60, n=3, center="dark"))
sns.set(style="dark")
sns.set(rc={'axes.facecolor':'#e0e4ed'}) # Blueberry Paste
HolmuskColors = sns.color_palette(["#1649bf", "#a2d729", "#192d5b"]) 
                         # Essentially Navy  Violent Lime  Sapphire
sns.set_palette(HolmuskColors)
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
jsonConfig = jsonref.load(open('../config/modules/figure1.json'))
logBase = config['logging']['logBase'] + '.modules.figure1.figure1'
dbName = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"data.pickle",'rb') 
    data = pickle.load(fileObjectLoad)   
    fileObjectLoad.close()

    df = data['df']

    print('='*40)
    print("Figure 1")
    print('='*40)

    dfFigure1Buffer = []

    # Generate table data
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

    # Put numerical labels above every bar
    for p in ax.patches: 
        height = p.get_height()
        ax.text(p.get_x()+p.get_width()/2.,
                height + 0.5,
                '{:1.1f}'.format(height),
                ha="center") 

    # Save and/or show plot
    plt.savefig(jsonConfig["outputs"]["saveFigPath"], dpi=600, bbox_inches = "tight")
    #plt.show()

    print("Saved to " + jsonConfig["outputs"]["saveFigPath"])

    return




