from logs import logDecorator as lD 
import jsonref, pprint
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
sns.set(style="dark")
from scipy import stats
import pickle

import dask.array as da

from lib.databaseIO import pgIO

config = jsonref.load(open('../config/config.json'))
module1_config = jsonref.load(open('../config/modules/module1.json'))
logBase = config['logging']['logBase'] + '.modules.module1.module1'

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

    try: # SET UP QUERY
        numSamples = str(module1_config["inputs"]["numSamples"]) 
        dbName = module1_config["inputs"]["dbName"]
        genRetrieve = pgIO.getDataIterator("select * from raw_data.allergies where (reaction is not null) \
                                            and (reaction not like '%nknown%') \
                                            and (reaction <> '?') \
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


    try: #LOAD THE PICKLE
        print("-"*20) 
        fileObjectLoad = open(module1_config["inputs"]["rawDataPath"],'rb') 
        readData = pickle.load(fileObjectLoad)   
        fileObjectLoad.close()
        print("Load from pickle")
        print(readData)

    except Exception as e:
        logger.error(f'Issue loading from pickle: " {e}')


    try: #CHECK DATA
        print("-"*20) 
        if(len(rawData)==len(readData)):
            print("Length check ok")
            print("Your thing is probably okay")
        else:
            logger.error(f'Save and load data different, something is wrong"')

    except Exception as e:
        logger.error(f'Error in length check (how?): " {e}')

    #genSend = pgIO.getDataIterator("create table jingwen.allergicReactions ();", dbName = module1_config["inputs"]["dbName"], chunks = 100)

    return



