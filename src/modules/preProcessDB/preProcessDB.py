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


@lD.log(logBase + '.oneHotDiagnoses')
def oneHotDiagnoses(logger):

    # Prepare filter sring
    dsmDiagnosesFilter = pd.read_csv(jsonConfig["inputs"]["dsmDiagnosesPath"])
    dsmSUDFilter       = pd.read_csv(jsonConfig["inputs"]["dsmSUDPath"])
    dsmQueryString     = '''
                         select id, siteid
                         '''
    # Create filter string
    for filterType in [dsmDiagnosesFilter, dsmSUDFilter]:
        for category in filterType: #REMOVE FLAG
            dsmQueryString += " ,count(case when " # 
            for dsmno in filterType[category]:
                if dsmno == dsmno:
                    dsmQueryString += " dsmno='" + str(dsmno) + "' or "
                    category = headerParse(category)

            dsmQueryString = dsmQueryString[:-3] + " then 1 end) as " + category

    dsmQueryString +=  '''
                                 from jingwen.temp3
                                 group by id, siteid
                                '''

    return dsmQueryString

@lD.log(logBase + '.relabelSQL')
def relabelSQL(logger, column, filterJSON):
    # Prepare filter sring
    filter = pd.read_csv(filterJSON)
    valueList = filter['category'].unique()
    relabelQueryStringList = []             

    tableName = jsonConfig["inputs"]["tableName"]
    schemaName = jsonConfig["inputs"]["schemaName"]

    for desiredCategory in valueList:
        if desiredCategory == desiredCategory:
            relabelQueryString = "UPDATE {} set {} = '{}' where ".format(schemaName + "." + tableName, column, desiredCategory)
            for idx, (value, category) in filter.iterrows():
                if category == desiredCategory: #not NAN
                    relabelQueryString += "(" + column + "='" + str(value) + "')or"
            relabelQueryStringList.append(relabelQueryString[:-2])
    return relabelQueryStringList


@lD.log(logBase + '.relabelComorbid')
def relabelComorbid(logger):

    # # Prepare filter sring
    # raceFilter = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])
    # raceList = raceFilter['category'].unique()
    # relabelComorbidQueryStringList = []             
      
    # # can be more efficient instead of running for every race
    # for desiredCategory in raceList:
    #     if desiredCategory == desiredCategory:
    #         relabelComorbidQueryString = "UPDATE jingwen.comorbid set race='" + desiredCategory + "' where "
    #         for idx, (race, category) in raceFilter.iterrows():
    #             if category == desiredCategory: #not NAN
    #                 relabelComorbidQueryString += "(race='" + str(race) + "')or"
    #         relabelComorbidQueryStringList.append(relabelComorbidQueryString[:-2])

    fullTableName = jsonConfig["inputs"]["schemaName"] + "." + jsonConfig["inputs"]["tableName"]

    relabelComorbidQueryStringList = []
    relabelComorbidQueryStringList += ( relabelSQL('race', jsonConfig["inputs"]["raceFilterPath"] ) )

    relabelComorbidQueryStringList.append('''
                                            UPDATE {}
                                            SET sex='F'
                                            WHERE (sex ilike 'F%');
                                            '''.format(fullTableName))

    relabelComorbidQueryStringList.append('''
                                            UPDATE {}
                                            SET sex='M'
                                            WHERE (sex ilike 'M%');
                                            '''.format(fullTableName))

    relabelComorbidQueryStringList.append('''
                                            UPDATE {}
                                            SET sex='Others'
                                            WHERE sex <> 'F' and sex <> 'M'
                                            '''.format(fullTableName))


    relabelComorbidQueryStringList.append('''
    ALTER TABLE {}
    ADD age_categorical text NULL;                                         
                                            '''.format(fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='1-11'
    WHERE CAST ({}.age AS INTEGER) <= 11 and CAST ({}.age AS INTEGER) >= 1
                                            '''.format(fullTableName, fullTableName, fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='12-17'
    WHERE CAST ({}.age AS INTEGER) <= 17 and CAST ({}.age AS INTEGER) >= 12
                                            '''.format(fullTableName, fullTableName, fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='18-34'
    WHERE CAST ({}.age AS INTEGER) <= 34 and CAST ({}.age AS INTEGER) >= 18
                                            '''.format(fullTableName, fullTableName, fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='35-49'
    WHERE CAST ({}.age AS INTEGER) <= 49 and CAST ({}.age AS INTEGER) >= 35
                                            '''.format(fullTableName, fullTableName, fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='50+'
    WHERE CAST ({}.age AS INTEGER) >= 50
                                            '''.format(fullTableName, fullTableName))


    relabelComorbidQueryStringList.append('''
    UPDATE {}
    SET age_categorical='0'
    WHERE CAST ({}.age AS INTEGER) = 0
                                            '''.format(fullTableName, fullTableName))

    return relabelComorbidQueryStringList

@lD.log(logBase + '.relabel')
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

    typePatientJoinQueryString          =   '''
                                            create table jingwen.temp1 as(
                                            select background.id, background.siteid, background.race, background.sex, 
                                            raw_data.typepatient.age, raw_data.typepatient.visit_type, raw_data.typepatient.created
                                            from
                                            (
                                            select id, siteid, race, sex from raw_data.background 
                                            where
                                            ''' + getFilterString('race', jsonConfig["inputs"]["raceFilterPath"]) + '''
                                            ) as background
                                            inner join raw_data.typepatient 
                                            on raw_data.typepatient.backgroundid = background.id and raw_data.typepatient.siteid = background.siteid
                                            );
                                            '''

    removeDuplicateVisitsQueryString    =   '''
                                            create table jingwen.temp2 as(
                                            with cte as
                                            (
                                            select *,
                                            ROW_NUMBER() OVER (PARTITION BY id, siteid ORDER BY created DESC) AS rn
                                            from jingwen.temp1
                                            )
                                            select id, siteid, race, sex, age, visit_type
                                            from cte
                                            where rn = 1    
                                            );
                                            '''     

    pdiagnoseJoinQueryString            =   '''
                                            create table jingwen.temp2_1 as(
                                            select jingwen.temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
                                            from jingwen.temp2
                                            inner join raw_data.pdiagnose 
                                            on raw_data.pdiagnose.backgroundid = jingwen.temp2.id and raw_data.pdiagnose.siteid = jingwen.temp2.siteid
                                            );
                                            '''

    pdiagnoseJoinQueryString2           =   '''
                                            create table jingwen.temp3 as(
                                            with cte as
                                            (
                                            select *,
                                            ROW_NUMBER() OVER (PARTITION BY id, dsmno, siteid) AS rn 
                                            from jingwen.temp2_1
                                            )
                                            select *
                                            from cte
                                            where rn = 1
                                            );
                                            '''
                                            # THIS IS BAD, CTE IS VERY WASTEFUL WHEN RECURSIVELY RUNNING SUBQUERIES BUT IT WORKS FOR NOW. T=7.64s

    oneHotDiagnosesQueryString          =   '''
                                            create table jingwen.temp4 as(
                                            ''' + oneHotDiagnoses() + '''
                                            );
                                            '''

    joinEverythingQueryString           =   '''
                                            create table jingwen.comorbid as(
                                            select * from(
                                            select jingwen.temp2.race, jingwen.temp2.sex, jingwen.temp2.age, jingwen.temp2.visit_type, jingwen.temp4.*
                                            from jingwen.temp4
                                            inner join jingwen.temp2
                                            on jingwen.temp4.id = jingwen.temp2.id and jingwen.temp4.siteid = jingwen.temp2.siteid 
                                            ) as x
                                            where CAST (age AS INTEGER) > 0
                                            and (visit_type ilike ('inpatient') or visit_type ilike ('outpatient'))
                                            );
                                            '''



    print('[preProcessDB] Running queries. This might take a while ...')

    # try:
        # print('Filter race and join with typepatient ... ', end = " ")
        # if pgIO.commitData(typePatientJoinQueryString , dbName = dbName):
        #     print('done\n')

        # print('Remove duplicate visits ... ', end = " ")
        # if pgIO.commitData(removeDuplicateVisitsQueryString , dbName = dbName):
        #     print('done\n')

        # print('Join with pdiagnose [1/2] ... ', end = " ")
        # if pgIO.commitData(pdiagnoseJoinQueryString , dbName = dbName):
        #     print('done\n')

        # print('Join with pdiagnose [2/2] ... ', end = " ")
        # if pgIO.commitData(pdiagnoseJoinQueryString2 , dbName = dbName):
        #     print('done\n')

        # print('One hot diagnoses and SUD ... ', end = " ")
        # if pgIO.commitData(oneHotDiagnosesQueryString , dbName = dbName):
        #     print('done\n')

        # print('Join everything ... ', end = " ")
        # if pgIO.commitData(joinEverythingQueryString , dbName = dbName):
        #     print('done\n')

        # print('Relabelling')
        # for relabelQuery in relabelComorbid():
        #     pgIO.commitData(relabelQuery , dbName = dbName)

    # except Exception as e:
    #     logger.error('Issue in query run. {}'.format(e))
    #     pass

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





