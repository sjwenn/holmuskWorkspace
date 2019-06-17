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

import time

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table2.json'))
logBase = config['logging']['logBase'] + '.modules.table2.table2'


@lD.log(logBase + '.CI')
def CI(logger, p, n, CL):
    SE = math.sqrt(p*(1-p)/n)  
    z_star = stats.norm.ppf((1-CL)/2)
    ME = z_star * SE
    return ME

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
    resultsDict: {dict}
        A dintionary containing information about the 
        command line arguments. These can be used for
        overwriting command line arguments as needed.
    '''
    dbName = jsonConfig["inputs"]["dbName"]

    if jsonConfig["params"]["useCacheFlag"] == 0: #check if redownload requested THIS IS NOT PEP8 BUT JSON NO WORK WITH PYTHON BOOL

        try: # SET UP QUERY
            genRetrieve = pgIO.getDataIterator("select * from jingwen.diagnoses" + ";",\
                                                dbName = dbName, chunks = 100)

            tempArray = [] #RUN THE QUERY
            for idx, data in enumerate(genRetrieve): 
                tempArray.append(da.from_array(data, chunks=(100,100)))
                print("Chunk: "+str(idx))
            rawData = da.concatenate(tempArray, axis=0)

        except Exception as e:
            logger.error(f'Issue with SQL query: " {e}')


        try: #SAVE THE PICKLE FOR DIAGNOSES
            fileSaveDiagnosisRaw = open(jsonConfig["outputs"]["intermediatePath"]+"sudRaw.pickle",'wb') 
            pickle.dump(rawData, fileSaveDiagnosisRaw)   
            fileSaveDiagnosisRaw.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE FOR DIAGNOSES
            fileLoadDiagnosisRaw = open(jsonConfig["inputs"]["intermediatePath"]+"sudRaw.pickle",'rb') 
            rawData = pickle.load(fileLoadDiagnosisRaw)   
            fileLoadDiagnosisRaw.close()

        except Exception as e:
            logger.error(f'Issue loading from pickle: " {e}')

    try: #CONVERT TO DATAFRAME (TODO: DIRECTLY APPEND FROM SQL TO FATAFRAME)
        df = dd.io.from_dask_array(rawData, columns=['id','siteid','race','sex','age_numeric','visit_type','age', 'dsm', 'diagnosis'])
        df = df.compute() #convert to pandas TODO be less inefficient

    except Exception as e:
        logger.error(f'Issue in convert dask array to dataframe: " {e}')

    dsm = pd.read_csv(jsonConfig["inputs"]["dsmPath"])

    raceList = df['race'].unique()
    ageList = df['age'].unique()
    sexList = df['sex'].unique()

    if jsonConfig["params"]["useCacheFlag"] == 0: #check if redownload requested
        try: #FORM SQL QUERY
            diagnosesBuf = [] #GET QUERY PER RACE
            for race in raceList:
                for age in ageList:
                    raceAgeTotal = pgIO.getAllData("select count(*) from jingwen.comorbid_updated where (race='"+race+"') and (age_categorical='"+age+"')" #TOTAL NO. PEOPLE IN RACE AND AGE
                            ,dbName = dbName).pop()[0] 
                    print((race + age + " done: ").ljust(12) + str(raceAgeTotal)) #DEBUG
                    for column in dsm: #REMOVE FLAG
                        queryString = "select count(distinct id) from jingwen.diagnoses where (race='"+race+"') and (age_categorical='"+age+"') and (" #BASE QUERY
                        for row in dsm[column]:
                            if row==row: #TEST IF NOT NAN
                                queryString = queryString + "(dsmno='"+str(row)+"')or" #ADD TO QUERY

                        queryString = queryString[:-2] + ")" #REMOVE LAST "OR"
                        valRetrieve = pgIO.getAllData(queryString,dbName = dbName).pop()[0]
                        diagnosesBuf.append([valRetrieve/raceAgeTotal*100, re.sub(r'\([^)]*\)', #REGEX TO REMOVE TEXT IN BRACKETS (CHILDHOOD-ONSET)
                                                                               '', column), race, age])
            diagnoses = pd.DataFrame(diagnosesBuf, columns=['%', 'Diagnosis', 'Race', 'Age'])

        except Exception as e:
            logger.error(f'Issue with frequency count " {e}')

        try: #SAVE THE PICKLE FOR DIAGNOSES
            fileSaveDiagnosisCount = open(jsonConfig["outputs"]["intermediatePath"]+"sudCount.pickle",'wb') 
            pickle.dump(diagnoses, fileSaveDiagnosisCount)   
            fileSaveDiagnosisCount.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE FOR DIAGNOSES
            fileLoadDiagnosisCount = open(jsonConfig["inputs"]["intermediatePath"]+"sudCount.pickle",'rb') 
            diagnoses = pickle.load(fileLoadDiagnosisCount)   
            fileLoadDiagnosisCount.close()

        except Exception as e:
            logger.error(f'Issue loading from pickle: " {e}')

    print(diagnoses[diagnoses['Race']=='AA'])
    
    try: #PLOT BARCHART
        plt.figure(figsize=(15,8)) 
        ax = sns.barplot(x="Diagnosis", y="%", hue="Race", data=diagnoses)
        plt.xticks(rotation=45)

        for p in ax.patches: #PUT NUMERICAL LABELS ABOVE BARS IN BAR CHART
            height = p.get_height()
            ax.text(p.get_x()+p.get_width()/2.,
                    height + 0.5,
                    '{:1.2f}'.format(height),
                    ha="center") 


        plt.savefig(jsonConfig["outputs"]["saveFigPath"], dpi=600, bbox_inches = "tight")

        if jsonConfig["params"]["showGraphFlag"] == 1:
            plt.show()
        
    except Exception as e:
        logger.error(f'Issue drawing barchart: " {e}')

    return




