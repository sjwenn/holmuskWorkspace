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
module1_config = jsonref.load(open('../config/modules/figure1.json'))
logBase = config['logging']['logBase'] + '.modules.figure1.figure1'


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
            dbName = module1_config["inputs"]["dbName"]
            genRetrieve = pgIO.getDataIterator("select * from jingwen.diagnoses" + ";",\
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
        df = dd.io.from_dask_array(rawData, columns=['id','siteid','race','sex','age_numeric','visit_type','age', 'dsm', 'diagnosis'])
        df = df.compute() #convert to pandas TODO be less inefficient

    except Exception as e:
        logger.error(f'Issue with frequency count: " {e}')

    dsm = pd.read_csv(module1_config["inputs"]["dsmPath"])
    test = dsm.isin(['296.00']).dropna(how='all')
    print(test[test].dropna())
    #df['dsm']



    return




