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

            tempArray = [] #BUFFER TO SAVE QUERY RESULT CHUNK
            for idx, data in enumerate(genRetrieve): 
                tempArray.append(da.from_array(data, chunks=(100,100)))
                print("Chunk: "+str(idx))
            rawData = da.concatenate(tempArray, axis=0)

        except Exception as e:
            logger.error(f'Issue with SQL query: " {e}')


        try: #SAVE THE PICKLE FOR sudCount
            fileSaveDiagnosisRaw = open(jsonConfig["outputs"]["intermediatePath"]+"sudRaw.pickle",'wb') 
            pickle.dump(rawData, fileSaveDiagnosisRaw)   
            fileSaveDiagnosisRaw.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE FOR sudCount
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
            sudCountBuf = [] #GET QUERY PER RACE
            for race in raceList:
                for age in np.append('Total', ageList):
                    if age != 'Total':
                        raceAgeTotalQueryString = "select count(*) from jingwen.comorbid_updated where (race='"+race+"') and (age_categorical='"+age+"')"
                        templateQueryString = "select count(distinct id) from jingwen.diagnoses where (race='"+race+"') and (age_categorical='"+age+"') and (" #BASE QUERY
                        moreThan2QueryString = "select count(*)from( select id, siteid from jingwen.diagnoses where (race='"+race+"') and (age_categorical='"+age+"') and ("
                    else:
                        raceAgeTotalQueryString = "select count(*) from jingwen.comorbid_updated where (race='"+race+"') " #TOTAL NO. PEOPLE IN RACE AND AGE
                        templateQueryString = "select count(distinct id) from jingwen.diagnoses where (race='"+race+"') and (" #BASE QUERY
                        moreThan2QueryString = "select count(*)from( select id, siteid from jingwen.diagnoses where (race='"+race+"') and ("

                    raceAgeTotal = pgIO.getAllData( raceAgeTotalQueryString, dbName = dbName).pop()[0] 

                    fullQueryString = templateQueryString
                    for column in dsm: #REMOVE FLAG
                        queryString = templateQueryString
                        for row in dsm[column]:
                            if row==row: #TEST IF NOT NAN
                                queryString += "(dsmno='"+str(row)+"')or" #ADD TO QUERY
                                fullQueryString += "(dsmno='"+str(row)+"')or"
                                moreThan2QueryString  += "(dsmno='"+str(row)+"')or"

                        queryString = queryString[:-2] + ")" #REMOVE LAST "OR"
                        valRetrieve = pgIO.getAllData(queryString,dbName = dbName).pop()[0]
                        sudCountBuf.append([round(valRetrieve/raceAgeTotal*100,1), column, race, age])

                    fullQueryString = fullQueryString[:-2] + ")"
                    valRetrieve = pgIO.getAllData(fullQueryString,dbName = dbName).pop()[0]
                    sudCountBuf.append([round(valRetrieve/raceAgeTotal*100,1), 'Any SUD', race, age])

                    moreThan2QueryString = moreThan2QueryString[:-2] + ")" + " group by id, siteid having count(*) > 1) as tmp" # POTENTIAL ISSUE: PRESENTING WITH SAME SUD MULTIPLE TIMES
                    valRetrieve = pgIO.getAllData(moreThan2QueryString,dbName = dbName).pop()[0]
                    sudCountBuf.append([round(valRetrieve/raceAgeTotal*100,1), '>= 2 SUDs', race, age])

                    print((race + age + " done: ").ljust(12) + str(raceAgeTotal)) #DEBUG

            sudCount = pd.DataFrame(sudCountBuf, columns=['%', 'SUD', 'Race', 'Age'])

        except Exception as e:
            logger.error(f'Issue with frequency count " {e}')

        try: #SAVE THE PICKLE FOR sudCount
            fileSaveDiagnosisCount = open(jsonConfig["outputs"]["intermediatePath"]+"sudCount.pickle",'wb') 
            pickle.dump(sudCount, fileSaveDiagnosisCount)   
            fileSaveDiagnosisCount.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE FOR sudCount
            fileLoadDiagnosisCount = open(jsonConfig["inputs"]["intermediatePath"]+"sudCount.pickle",'rb') 
            sudCount = pickle.load(fileLoadDiagnosisCount)   
            fileLoadDiagnosisCount.close()

        except Exception as e:
            logger.error(f'Issue loading from pickle: " {e}')
    
    try:
        tableString = ""
        sudCount = sudCount[sudCount['Age']!='1-11']
        
        for race in raceList:
            tableSudCount = sudCount[sudCount['Race']==race].pivot(index='SUD', columns='Age')['%']

            #MOVE TOTAL COLUMN TO FRONT
            cols = tableSudCount.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            tableSudCount = tableSudCount[cols]

            # MAKE 'ANY SUD' FIRST INDEX
            tableSudCountIndices = tableSudCount.index.tolist()
            tableSudCountIndices.remove('Any SUD')
            tableSudCountIndices.insert(0, 'Any SUD')
            tableSudCount=tableSudCount.reindex(tableSudCountIndices)


            tableString += race + "\n"
            tableString += tabulate(tableSudCount , tablefmt="pipe", headers="keys")
            tableString += "\n\n"

        print(tableString)
        
    except Exception as e:
        logger.error(f'Issue generating table 2: " {e}')


    try: #SAVE THE PICKLE OF TABLE1STRING
        fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"table2String.pickle",'wb') 
        pickle.dump(tableString, fileObjectSave)   
        fileObjectSave.close()

    except Exception as e:
        logger.error(f'Issue saving to pickle: " {e}')

    return




