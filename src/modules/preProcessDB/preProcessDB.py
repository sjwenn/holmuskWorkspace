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

config     = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/preProcessDB.json'))
logBase    = config['logging']['logBase'] + '.modules.preProcessDB.preProcessDB'
dbName     = jsonConfig["inputs"]["dbName"]

@lD.log(logBase + '.headerParse')
def headerParse(logger, headers):

    headers = headers.replace('/', '_').replace(' ', '_').replace('-', '_').replace(',', '_')
    headers = re.sub(r'\([^)]*\)','', headers)
    headers = headers.lower()

    return headers


    
@lD.log(logBase + '.getFilterString')
def getFilterString(logger, column, filterJSON):

    filter = pd.read_csv(filterJSON)

    # Create filter string
    queryString = '('

    for idx, (value, category) in filter.iterrows():
        if category == category: #not NAN
            queryString += "({}='{}')or".format(column, value)

    # Remove last 'or'
    queryString = queryString[:-2] + ")"

    return queryString


@lD.log(logBase + '.relabelSQL')
def relabel(logger, df, column, filterJSON):

    filter = pd.read_csv(filterJSON)         

    for idx, (value, category) in filter.iterrows():
        if category == category and value == value:
            df[column] = df[column].replace(value, category)

    return df

@lD.log(logBase + '.pdiagnoseJoin ')
def pdiagnoseJoin(logger):
    queryString = 'select id, siteid from jingwen.temp2'
    pgIO.getAllData(queryString, dbName = dbName)

    return

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    raceList         = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])['category'].unique()
    SUDList          = pd.read_csv(jsonConfig["inputs"]["dsmSUDPath"]).columns.tolist()
    diagnosesList    = pd.read_csv(jsonConfig["inputs"]["dsmDiagnosesPath"]).columns.tolist()

    rawSUDList       = SUDList
    rawDiagnosesList = diagnosesList

    SUDList          = [headerParse(item) for item in SUDList]
    diagnosesList    = [headerParse(item) for item in diagnosesList]


    print('[preProcessDB] Running queries. This might take a while ...')
    
    raceFilter = getFilterString('race',jsonConfig["inputs"]["raceFilterPath"] )
    settingFilter = getFilterString('visit_type',jsonConfig["inputs"]["settingFilterPath"] )

    d = '''
        SELECT background.id, background.siteid, background.race, background.sex, 
        raw_data.typepatient.age, raw_data.typepatient.visit_type
        FROM
        (
            select id, siteid, race, sex from raw_data.background 
            where {}
        ) AS background
        INNER JOIN raw_data.typepatient 
        ON raw_data.typepatient.backgroundid = background.id and raw_data.typepatient.siteid = background.siteid
        '''.format(raceFilter)
    print(d)
    #a = pgIO.getAllData(d, dbName = dbName)
    print(a)


    schemaName = jsonConfig["inputs"]["schemaName"]
    tableName  = jsonConfig["inputs"]["tableName"]

    genRetrieve = pgIO.getDataIterator("select * from " + schemaName + "." + tableName, 
                                        dbName = dbName, 
                                        chunks = 100)

    dbColumnQueryString =       '''
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = '{}'
                                AND table_name = '{}'
                                '''.format(schemaName, tableName)

    dbColumns = pgIO.getAllData(dbColumnQueryString, dbName = dbName)

    dbColumns = [item[0] for item in dbColumns]

    tempArray = []
    for idx, data in enumerate(genRetrieve):
        tempArray += data
        print("Chunk: "+str(idx))
    
    rawData = pd.DataFrame(data = tempArray, columns = dbColumns)

    rawData = relabel(rawData, 'race', jsonConfig["inputs"]["raceFilterPath"])
    rawData = relabel(rawData, 'visit_type', jsonConfig["inputs"]["settingFilterPath"])

    try: #Save pickle to be sent to 'getUsefulInfo.py'
        fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"db.pickle",'wb') 
        miscData = [SUDList, diagnosesList, rawSUDList, rawDiagnosesList]
        pickle.dump((miscData, rawData), fileObjectSave)   
        fileObjectSave.close()

    except Exception as e:
        logger.error(f'Issue saving to pickle: " {e}')

    return
