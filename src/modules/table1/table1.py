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

import dask.array as da
import dask.dataframe as dd
import pandas as pd

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
module1_config = jsonref.load(open('../config/modules/table1.json'))
logBase = config['logging']['logBase'] + '.modules.table1.table1'


@lD.log(logBase + '.CI')
def CI(logger, p, n, CL):
    SE = math.sqrt(p*(1-p)/n)
    z_star = stats.norm.ppf((1-CL)/2)
    ME = z_star * SE
    return ME

@lD.log(logBase + '.plotTable1')
def plotTable1(logger, df):
    try: #COLUMN 'ALL'
        def listStuff(out):
            for idx, (label, value) in enumerate(out):
                print( label.ljust(6), end = ": " )
                print( value )
            return

        chi2bufAge = np.zeros((3,5)) #race * category
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
            chi2bufAge[0][idx] = value

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


    # try:
    #     numSamples = module1_config["inputs"]["numSamples"]
    #     distrabution = module1_config["inputs"]["distrabution"]

    #     if distrabution == 'Gaussian':
    #         data =  (np.random.normal(0, 1, numSamples), np.random.normal(0, 1, numSamples))
    #     else:
    #         data = None

    #     ax = sns.jointplot(data[0], data[1], alpha=0.3)
        
    #     slope, intercept, r_value, p_value, std_err = stats.linregress(data[0], data[1])
    #     xi = np.linspace(-3, 3, num=20000)
    #     ax.ax_joint.plot(xi, slope*xi+intercept, color=(0.0, 0.48, 0.1), dashes=(5, 2))

    if module1_config["params"]["useCacheFlag"] == 0: #check if redownload requested THIS IS NOT PEP8 BUT JSON NO WORK WITH PYTHON BOOL

        try: # SET UP QUERY
            numSamples = str(module1_config["inputs"]["numSamples"]) 
            dbName = module1_config["inputs"]["dbName"]
            genRetrieve = pgIO.getDataIterator("select * from jingwen.comorbid \
                                                limit " + numSamples + ";",\
                                                dbName = dbName, chunks = 100)

            tempArray = [] #RUN THE QUERY
            for idx, data in enumerate(genRetrieve): 
                tempArray.append(da.from_array(data, chunks=(100,100)))
                print("Chunk: "+str(idx))
            rawData = da.concatenate(tempArray, axis=0)

        except Exception as e:
            logger.error(f'Issue with SQL query: " {e}')


        try: #SAVE THE PICKLE
            print("-"*20) 
            print("Saved to pickle")
            print(rawData)
            fileObjectSave = open(module1_config["outputs"]["rawDataPath"],'wb') 
            pickle.dump(rawData, fileObjectSave)   
            fileObjectSave.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    else:
        try: #LOAD THE PICKLE
            print("-"*20) 
            fileObjectLoad = open(module1_config["inputs"]["rawDataPath"],'rb') 
            rawData = pickle.load(fileObjectLoad)   
            fileObjectLoad.close()
            print("Load from pickle")
            print(rawData)

        except Exception as e:
            logger.error(f'Issue loading from pickle: " {e}')


    try: #CONVERT TO DATAFRAME (TODO: DIRECTLY APPEND FROM SQL TO FATAFRAME)
        df = dd.io.from_dask_array(rawData, columns=['id','siteid','race','sex','age_numeric','visit_type','age'])
        df.compute()

    except Exception as e:
            logger.error(f'Issue with frequency count: " {e}')

    plotTable1(df)




    return




