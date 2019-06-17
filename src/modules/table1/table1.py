from logs import logDecorator as lD 
import jsonref, pprint
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
sns.set(style="dark")
from scipy import stats
from scipy.stats import chi2
from scipy.stats import chi2_contingency
import pickle
import math
from tabulate import tabulate

import dask.array as da
import dask.dataframe as dd
import pandas as pd

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/table1.json'))
logBase = config['logging']['logBase'] + '.modules.table1.table1'


@lD.log(logBase + '.CI')
def CI(logger, p, n, CL):
    '''CI
    
    Confidence interval
    
    Decorators:
        lD.log
    
    Arguments:
        logger -- Logs errors
        p  -- Probability
        n  -- NUmber of samples
        CL -- Confidence (e.g. 0.95)
    
    Returns:
        [type] -- [description]
    '''
    SE = math.sqrt(p*(1-p)/n)
    z_star = stats.norm.ppf((1-CL)/2)
    ME = z_star * SE
    return ME

@lD.log(logBase + '.plotTable1MD')
def fetchTable1MD(logger, df):
    '''fetchTable1MD
    
    Generate table
    
    Decorators:
        lD.log
    
    Arguments:
        logger {[type]} -- [description]
        df {[type]} -- [description]
    
    Returns:
        string -- Markdown string of table 1
    '''
    tableString = ""

    raceList = df['race'].unique().compute()
    ageList = df['age'].unique().compute()
    sexList = df['sex'].unique().compute()

    raceCounts = df['race'].value_counts().compute().to_frame().transpose()

    try: #COLUMN 'ALL'
        valAge = pd.DataFrame(columns=ageList, index=raceList) # FOR USE IN CHI2
        valSex = pd.DataFrame(columns=sexList, index=raceList) # FOR USE IN CHI2
        tableAge = pd.DataFrame(columns=ageList, index=raceList)
        tableSex = pd.DataFrame(columns=sexList, index=raceList)

        tableString += "### Overview\n"
        tableString += "#### Total Sample Size = " + str(len(df.index)) + "\n"

    except Exception as e:
        logger.error(f'Issue with column "All": " {e}')
        return

    try: #Table 1: Age
        for race in raceList:
            n = df['race'].value_counts().compute().to_dict()[race]

            out = df.loc[df['race'] == race]['age'].value_counts().compute().to_dict().items()
            for (label, value) in out:
                valAge.at[race,label] = value
                tableAge.at[race,label] = str(round(value/n*100,1)) + " (" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                                                                                    + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")" 
        tableAge = tableAge.fillna(value="0.0 (0.0-0.0)").transpose().sort_index() 
        tableAge.insert(loc=0, column='All', value = df['age'].value_counts().compute().to_frame())


    except Exception as e:
        logger.error(f'Issue with Table 1 (Age): " {e}')
        return


    try: #Table 1: Sex
        for race in raceList:
            n = df['race'].value_counts().compute().to_dict()[race]

            out = df.loc[df['race'] == race]['sex'].value_counts().compute().to_dict().items()

            for (label, value) in out:
                valSex.at[race,label] = value
                tableSex.at[race,label] = str(round(value/n*100,1)) + " (" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                                                                                    + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")" 
            
        tableSex = tableSex.fillna(value="0.0 (0.0-0.0)").transpose()
        tableSex.insert(loc=0, column='All', value = df['sex'].value_counts().compute().to_frame())
                                        
    except Exception as e:
        logger.error(f'Issue with Table 1 (Sex): " {e}')

    try: #Table 1 Output String
        tableString += "### Age \n" \
                    + tabulate(tableAge, tablefmt="pipe", headers="keys") \
                    + "\n\n### Sex \n" \
                    + tabulate(tableSex , tablefmt="pipe", headers="keys")

        tableString = tableString.replace("1-11", "**1-11**").replace("12-17", "**12-17**") \
                                 .replace("18-34", "**18-34**").replace("35-49", "**35-49**") \
                                 .replace("50+", "**50+**") \
                                .replace("MR", "MR ("+str(raceCounts['MR'][0])+")") \
                                 .replace("AA", "AA ("+str(raceCounts['AA'][0])+")") \
                                 .replace("NHPI", "NHPI ("+str(raceCounts['NHPI'][0])+")") \
                                 .replace("M", "**M**").replace("F", "**F**").replace("Others", "**Others**")\
                                 .replace("age", "Age").replace("sex", "Sex").replace("race", "Race") 
                                 # TODO: REPLACE RACECOUNTS TO LOOK BETTER


    except Exception as e:
        logger.error(f'Issue with Table 1 output string: " {e}')
        tableString =  'Issue with Table 1 output string: check log'

    return tableString

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

    if jsonConfig["params"]["useCacheFlag"] == 0: #check if redownload requested THIS IS NOT PEP8 BUT JSON NO WORK WITH PYTHON BOOL

        try: # SET UP QUERY
            maxNumSamples = str(jsonConfig["inputs"]["maxNumSamples"]) 
            dbName = jsonConfig["inputs"]["dbName"]
            genRetrieve = pgIO.getDataIterator("select * from jingwen.comorbid_updated \
                                                limit " + maxNumSamples + ";",\
                                                dbName = dbName, chunks = 100)

            tempArray = [] #RUN THE QUERY
            for idx, data in enumerate(genRetrieve): 
                tempArray.append(da.from_array(data, chunks=(100,100)))
                print("Chunk: "+str(idx))
            rawData = da.concatenate(tempArray, axis=0)

        except Exception as e:
            logger.error(f'Issue with SQL query: " {e}')


        try: #SAVE THE PICKLE
            fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"allergicReactions.pickle",'wb') 
            pickle.dump(rawData, fileObjectSave)   
            fileObjectSave.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE
            fileObjectLoad = open(jsonConfig["inputs"]["intermediatePath"]+"allergicReactions.pickle",'rb') 
            rawData = pickle.load(fileObjectLoad)   
            fileObjectLoad.close()

        except Exception as e:
            logger.error(f'Issue loading from pickle: " {e}')


    try: #CONVERT TO DATAFRAME (TODO: DIRECTLY APPEND FROM SQL TO FATAFRAME)
        df = dd.io.from_dask_array(rawData, columns=['id','siteid','race','sex','age_numeric','visit_type','age'])
        df.compute()

    except Exception as e:
            logger.error(f'Issue with frequency count: " {e}')

    try: #SAVE THE PICKLE OF TABLE1STRING
        fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"table1String.pickle",'wb') 
        pickle.dump(fetchTable1MD(df), fileObjectSave)   
        fileObjectSave.close()

    except Exception as e:
        logger.error(f'Issue saving to pickle: " {e}')


    return








@lD.log(logBase + '.plotTable1CLI')
def plotTable1CLI(logger, df): 
#  ____                           _       _           _ 
# |  _ \  ___ _ __  _ __ ___  ___(_) __ _| |_ ___  __| |
# | | | |/ _ \ '_ \| '__/ _ \/ __| |/ _` | __/ _ \/ _` |
# | |_| |  __/ |_) | | |  __/ (__| | (_| | ||  __/ (_| |
# |____/ \___| .__/|_|  \___|\___|_|\__,_|\__\___|\__,_|
#            |_| 
    '''plotTable1CLI
    
    Command line printing of table1.
    
    Decorators:
        lD.log
    
    Arguments:
        logger : {logging.Logger}
            The logger used for logging error information

        df : {pd.dataframe}
            Dataframe containing characteristics of Asian 
            Americans, Native Hawaiians/Pacific Islanders, 
            and mixed-race people

    '''
    try: #COLUMN 'ALL'
        def listStuff(out):
            for idx, (label, value) in enumerate(out):
                print( label.ljust(6), end = ": " )
                print( value )
            return

        chi2bufAge = np.zeros((3,5))
        chi2bufSex = np.zeros((3,3)) #race * category

        print('-'*100)
        print('All')
        out = df['race'].value_counts().compute().to_dict().items()
        print('\nRace')
        listStuff(out)

        out = df['age'].value_counts().compute().to_dict().items()
        print('\nAge') 
        listStuff(out)

        out = df['sex'].value_counts().compute().to_dict().items()
        print('\nSex') 
        listStuff(out)

        out = df['visit_type'].value_counts().compute().to_dict().items()
        print('\nSetting') 
        listStuff(out)

    except Exception as e:
        logger.error(f'Issue with printing column "All": " {e}')
        return

    try: #COLUMN 'AA'
        print('-'*100)
        print('Asian American')
        n = df['race'].value_counts().compute().to_dict()['AA']

        out = df.loc[df['race'] == 'AA']['age'].value_counts().compute().to_dict().items()
        print('\nAge: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufAge[label] = value

        out = df.loc[df['race'] == 'AA']['sex'].value_counts().compute().to_dict().items()
        print('\nSex: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufSex[0][idx] = value

    except Exception as e:
        logger.error(f'Issue with printing column "AA": " {e}')
        return

    try: #COLUMN 'NHPI'
        print('-'*100)
        print('Native Hawaiian / Pacific Islander')
        n = df['race'].value_counts().compute().to_dict()['NHPI']

        out = df.loc[df['race'] == 'NHPI']['age'].value_counts().compute().to_dict().items()
        print('\nAge: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufAge[1][idx] = value

        out = df.loc[df['race'] == 'NHPI']['sex'].value_counts().compute().to_dict().items()
        print('\nSex: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufSex[1][idx] = value
            
    except Exception as e:
        logger.error(f'Issue with printing column "NHPI": " {e}')
        return

    try: #COLUMN 'MR'
        print('-'*100)
        print('Mixed Race')
        n = df['race'].value_counts().compute().to_dict()['MR']

        out = df.loc[df['race'] == 'MR']['age'].value_counts().compute().to_dict().items()
        print('\nAge: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufAge[2][idx] = value

        out = df.loc[df['race'] == 'MR']['sex'].value_counts().compute().to_dict().items()
        print('\nSex: ') 
        for idx, (label, value) in enumerate(out):
            print( label.ljust(6), end = ": " )
            print( str(round(value/n*100,1)).ljust(6), end = " " )
            print(  "(" + str(round((value/n+CI(value/n, n, 0.95))*100,1)) + "-" \
                        + str(round((value/n-CI(value/n, n, 0.95))*100,1)) + ")"   )
            chi2bufSex[2][idx] = value

        print(chi2bufAge)

        print('-'*100)
        print("Chi2")
        print("\nAge ".ljust(5))
        chi2, p, dof, ex = chi2_contingency(chi2bufAge)
        print("Df: " + str(dof))
        print("P-value: "+str(p))   

        print("\nSex: ".ljust(5))
        chi2, p, dof, ex = chi2_contingency(chi2bufSex)
        print("Df: " + str(dof))
        print("P-value: "+str(p))
        print('-'*100)
            
    except Exception as e:
        logger.error(f'Issue with printing column "MR": " {e}')
        return






