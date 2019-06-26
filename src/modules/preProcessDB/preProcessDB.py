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
    queryString     = '''
                         select id, siteid
                         '''
    # Create filter string
    for filterType in [dsmDiagnosesFilter, dsmSUDFilter]:
        for category in filterType: #REMOVE FLAG
            queryString += " ,count(case when " # 
            for dsmno in filterType[category]:
                if dsmno == dsmno:
                    queryString += " dsmno='" + str(dsmno) + "' or "
                    category = headerParse(category)

            queryString = queryString[:-3] + " then 1 end) as " + category

    queryString +=   '''
                     from jingwen.temp3
                     group by id, siteid
                     '''

    return queryString

@lD.log(logBase + '.relabelSQL')
def getRelabelString(logger, column, filterJSON):

    # Prepare filter sring
    filter = pd.read_csv(filterJSON)
    valueList = filter['category'].unique()
    queryString = []             

    tableName = jsonConfig["inputs"]["tableName"]
    schemaName = jsonConfig["inputs"]["schemaName"]

    for desiredCategory in valueList:
        if desiredCategory == desiredCategory:
            relabelQueryString = "UPDATE {} set {} = '{}' where ".format(schemaName + "." + tableName, column, desiredCategory)
            for idx, (value, category) in filter.iterrows():
                if category == desiredCategory: #not NAN
                    relabelQueryString += "(" + column + "='" + str(value) + "')or"

            queryString.append(relabelQueryString[:-2])

    return queryString

@lD.log(logBase + '.relabel')
def relabel(logger, df, column, filterJSON):

    filter = pd.read_csv(filterJSON)         

    for idx, (value, category) in filter.iterrows():
        if category == category and value == value:
            df[column] = df[column].replace(value, category)

    return df

@lD.log(logBase + '.relabel')
def checkTableExistence(logger, schemaName, tableName):
    doesExistQueryString =      '''
                                SELECT EXISTS 
                                (
                                SELECT 1
                                FROM   information_schema.tables 
                                WHERE  table_schema = '{}'
                                AND    table_name = '{}'
                                );
                                '''.format(schemaName, tableName)

    doesExistFlag = pgIO.getAllData(doesExistQueryString, dbName = dbName )[0][0]

    return doesExistFlag

@lD.log(logBase + '.relabel')
def createTable(logger, schemaName, tableName, createTableQueryString, existsTableQueryString = ''):

    if checkTableExistence(schemaName, tableName):
        if existsTableQueryString != '':
            pgIO.commitData(existsTableQueryString, dbName = dbName )
        return False
    else:
        pgIO.commitData(createTableQueryString, dbName = dbName )
        return True



@lD.log(logBase + '.subroutineJoinDiagnoses')
def subroutineJoinTypepatient(logger):

    def recursiveQuery(totalRows, recursionChunkSize = 1000, scalingFactor = 0.1, ttl = 5 ):

        getFilterString('race', jsonConfig["inputs"]["raceFilterPath"])
        getFilterString('sex', jsonConfig["inputs"]["sexFilterPath"])
        getFilterString('sex', jsonConfig["inputs"]["sexFilterPath"])

        for idx in range(0, totalRows, recursionChunkSize):
            lowerBound = idx
            upperBound = idx + recursionChunkSize

            queryString =       '''
                                INSERT into jingwen.temp2
                                with cte as
                                (
                                select *,
                                ROW_NUMBER() OVER (PARTITION BY id, siteid ORDER BY age asc) AS rn
                                from 
                                (
                                select background.id, background.siteid, background.race, background.sex, 
                                typepatient.age, typepatient.visit_type
                                from
                                (
                                select id, siteid, race, sex from raw_data.background 
                                where CAST (id as INTEGER) >= {} and CAST (id as INTEGER) < {}
                                and
                                '''.format(lowerBound, upperBound) + getFilterString('race', jsonConfig["inputs"]["raceFilterPath"]) + '''
                                and
                                ''' + getFilterString('sex', jsonConfig["inputs"]["sexFilterPath"]) + '''
                                ) as background
                                inner join 
                                (
                                select backgroundid, siteid, age, visit_type, created from raw_data.typepatient
                                where 
                                ''' + getFilterString('visit_type', jsonConfig["inputs"]["settingFilterPath"]) + '''
                                and (age IS NOT NULL )
                                ) as typepatient
                                on typepatient.backgroundid = background.id and typepatient.siteid = background.siteid
                                )as x
                                )
                                select id, siteid, race, sex, age, visit_type
                                from cte
                                where rn = 1    
                                                  
                                '''.format(lowerBound, upperBound)
        
            isSuccesfulFlag = pgIO.commitData(queryString , dbName = dbName)
            print("ID {} to {}: {}".format(lowerBound, upperBound, isSuccesfulFlag))

        return True


    schemaName = jsonConfig["inputs"]["schemaName"]
    tableName  = jsonConfig["inputs"]["tableName"]
    fullTableName = schemaName + "." + tableName

    createTemp2String =     '''
                            CREATE TABLE jingwen.temp2 (
                            id text NULL,
                            siteid text NULL,
                            race text NULL,
                            sex text NULL,
                            age text NULL,
                            visit_type text NULL,
                            );
                            '''
    if createTable(schemaName, 'temp2', createTemp2String):

        maxID = pgIO.getAllData("select max(CAST (id as INTEGER)) from raw_data.background", dbName = dbName )[0][0]
        print(maxID)
        recursiveQuery(maxID)

    else:
        print("temp2 already exists")
        

    return




@lD.log(logBase + '.subroutineJoinDiagnoses')
def subroutineJoinDiagnoses(logger):

    def recursiveQuery(totalRows, recursionChunkSize = 1000, scalingFactor = 0.1, ttl = 5 ):

        for idx in range(0, totalRows, recursionChunkSize):
            lowerBound = idx
            upperBound = idx + recursionChunkSize

            queryString =       '''
                                INSERT into jingwen.temp3
                                SELECT temp2.*, y.dsmno
                                from jingwen.temp2 as temp2
                                inner join
                                (
                                    select  id, dsmno, siteid
                                    from    
                                    (
                                        select temp2.id, temp2.siteid, raw_data.pdiagnose.dsmno
                                        from
                                        (
                                            select id, siteid from jingwen.temp2
                                            where CAST (id as INTEGER) >= {} and CAST (id as INTEGER) < {}
                                        ) as temp2
                                        inner join raw_data.pdiagnose 
                                        on raw_data.pdiagnose.backgroundid = temp2.id and raw_data.pdiagnose.siteid = temp2.siteid
                                    ) as x
                                    group by id, dsmno, siteid
                                ) as y
                                on y.id = temp2.id and y.siteid = temp2.siteid
                                '''.format(lowerBound, upperBound)
        
            isSuccesfulFlag = pgIO.commitData(queryString , dbName = dbName)
            print("ID {} to {}: {}".format(lowerBound, upperBound, isSuccesfulFlag))

        return True


    schemaName = jsonConfig["inputs"]["schemaName"]
    tableName  = jsonConfig["inputs"]["tableName"]
    fullTableName = schemaName + "." + tableName

    createTemp3String =     '''
                            CREATE TABLE jingwen.temp3 (
                            id text NULL,
                            siteid text NULL,
                            race text NULL,
                            sex text NULL,
                            age text NULL,
                            visit_type text NULL,
                            dsmno text NULL
                            );
                            '''
    if createTable(schemaName, 'temp3', createTemp3String):

        maxID = pgIO.getAllData("select max(CAST (id as INTEGER)) from jingwen.temp2", dbName = dbName )[0][0]
        print(maxID)
        recursiveQuery(maxID)

    else:
        print("temp3 already exists")
        

    return



@lD.log(logBase + '.relabelComorbid')
def subroutineRelabelComorbid(logger):

    fullTableName = jsonConfig["inputs"]["schemaName"] + "." + jsonConfig["inputs"]["tableName"]

    queryStringList = []
    queryStringList += ( getRelabelString('race',          jsonConfig["inputs"]["raceFilterPath"] ) )
    queryStringList += ( getRelabelString('visit_type',    jsonConfig["inputs"]["settingFilterPath"] ) )
    queryStringList += ( getRelabelString('sex',           jsonConfig["inputs"]["sexFilterPath"] ) )


    queryStringList.append('''
    ALTER TABLE {}
    ADD age_categorical text NULL;                                         
                                            '''.format(fullTableName))

    for item in [[1,11], [12,17], [18,34], [35,49]]:
        queryStringList.append('''
        UPDATE {}
        SET age_categorical='{}-{}'
        WHERE CAST ({}.age AS INTEGER) <= {} and CAST ({}.age AS INTEGER) >= {}
        '''.format( fullTableName, item[0], item[1],  fullTableName, item[1], fullTableName, item[0]))

    queryStringList.append('''
    UPDATE {}
    SET age_categorical='50+'
    WHERE CAST ({}.age AS INTEGER) >= 50
                                            '''.format(fullTableName, fullTableName))

    queryStringList.append('''
    UPDATE {}
    SET age_categorical='0'
    WHERE CAST ({}.age AS INTEGER) = 0
                                            '''.format(fullTableName, fullTableName))
    for relabelQuery in queryStringList:
        pgIO.commitData(relabelQuery , dbName = dbName)

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
                                            typepatient.age, typepatient.visit_type, typepatient.created
                                            from
                                            (
                                            select id, siteid, race, sex from raw_data.background 
                                            where
                                            ''' + getFilterString('race', jsonConfig["inputs"]["raceFilterPath"]) + '''
                                            and
                                            ''' + getFilterString('sex', jsonConfig["inputs"]["sexFilterPath"]) + '''
                                            ) as background
                                            inner join 
                                            (
                                            select backgroundid, siteid, age, visit_type, created from raw_data.typepatient
                                            where 
                                            ''' + getFilterString('visit_type', jsonConfig["inputs"]["settingFilterPath"]) + '''
                                            and (age IS NOT NULL )
                                            ) as typepatient
                                            on typepatient.backgroundid = background.id and typepatient.siteid = background.siteid
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

    oneHotDiagnosesQueryString          =   '''
                                            create table jingwen.temp4 as(
                                            ''' + oneHotDiagnoses() + '''
                                            );
                                            '''

    joinEverythingQueryString           =   '''
                                            create table jingwen.comorbid as
                                            (
                                            select * from
                                            (
                                            select jingwen.temp2.race, jingwen.temp2.sex, jingwen.temp2.age, jingwen.temp2.visit_type, jingwen.temp4.*
                                            from jingwen.temp4
                                            inner join jingwen.temp2
                                            on jingwen.temp4.id = jingwen.temp2.id and jingwen.temp4.siteid = jingwen.temp2.siteid 
                                            ) as x
                                            where CAST (age AS INTEGER) > 0
                                            and (''' + getFilterString('visit_type', jsonConfig["inputs"]["settingFilterPath"]) + ''')
                                            );
                                            '''



    schemaName = jsonConfig["inputs"]["schemaName"]
    tableName  = jsonConfig["inputs"]["tableName"]
    fullTableName = schemaName + "." + tableName


    subroutineJoinTypepatient()


    # if not checkTableExistence(schemaName, tableName):

    #     print('[preProcessDB] {}.{} not found. Generating now.'.format(schemaName, tableName))

    #     print('[preProcessDB] Running queries. This might take a while ...')

    #     print('Filter race and join with typepatient ... ', end = " ")
    #     if pgIO.commitData(typePatientJoinQueryString , dbName = dbName):
    #         print('done\n')
    #     else:
    #         print('fail\n')

    #     print('Remove duplicate visits ... ', end = " ")
    #     if pgIO.commitData(removeDuplicateVisitsQueryString , dbName = dbName):
    #         print('done\n')
    #     else:
    #         print('fail\n')

    #     print('Join with pdiagnose ... ', end = " ")
    #     subroutineJoinDiagnoses()
    #     print('done\n')

    #     print('One hot diagnoses and SUD ... ', end = " ")
    #     if pgIO.commitData(oneHotDiagnosesQueryString , dbName = dbName):
    #         print('done\n')
    #     else:
    #         print('fail\n')

    #     print('Join everything ... ', end = " ")
    #     if pgIO.commitData(joinEverythingQueryString , dbName = dbName):
    #         print('done\n')
    #     else:
    #         print('fail\n')

    #     print('Relabelling ...', end = " ")
    #     subroutineRelabelComorbid()
    #     print('done\n')

    # else:

    #     print('[preProcessDB] {}.{} found. Skipping generation'.format(schemaName, tableName))

    genRetrieve = pgIO.getDataIterator("select * from " + fullTableName, 
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





