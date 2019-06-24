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


    
@lD.log(logBase + '.filterRace')
def filterRace(logger):

    # Prepare filter sring
    raceFilter = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])
    filterRaceQueryString = '''
                            select id, siteid, race, sex from raw_data.background 
                            where (
                            '''

    # Create filter string
    for idx, (race, category) in raceFilter.iterrows():
        if category == category: #not NAN
            filterRaceQueryString += "(race='" + race + "')or"

    # Remove last 'or'
    filterRaceQueryString = filterRaceQueryString[:-2] + ")"

    return filterRaceQueryString



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



# @lD.log(logBase + '.clearTemporaryTables')
# def clearTemporaryTables(logger):
#     return



@lD.log(logBase + '.relabelComorbid')
def relabelComorbid(logger):

    # Prepare filter sring
    raceFilter = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])
    raceList = raceFilter['category'].unique()
    relabelComorbidQueryStringList = []             
      
    # can be more efficient instead of running for every race
    for desiredCategory in raceList:
        if desiredCategory == desiredCategory:
            relabelComorbidQueryString = "UPDATE jingwen.comorbid set race='" + desiredCategory + "' where "
            for idx, (race, category) in raceFilter.iterrows():
                if category == desiredCategory: #not NAN
                    relabelComorbidQueryString += "(race='" + str(race) + "')or"
            relabelComorbidQueryStringList.append(relabelComorbidQueryString[:-2])

    relabelComorbidQueryStringList.append('''
                                            UPDATE jingwen.comorbid
                                            SET sex='F'
                                            WHERE (sex ilike 'F%');
                                            ''')

    relabelComorbidQueryStringList.append('''
                                            UPDATE jingwen.comorbid
                                            SET sex='M'
                                            WHERE (sex ilike 'M%');
                                            ''')

    relabelComorbidQueryStringList.append('''
                                            UPDATE jingwen.comorbid
                                            SET sex='Others'
                                            WHERE sex <> 'F' and sex <> 'M'
                                            ''')


    relabelComorbidQueryStringList.append('''
    ALTER TABLE jingwen.comorbid
    ADD age_categorical text NULL;                                         
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='1-11'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) <= 11 and CAST (jingwen.comorbid.age AS INTEGER) >= 1
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='12-17'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) <= 17 and CAST (jingwen.comorbid.age AS INTEGER) >= 12
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='18-34'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) <= 34 and CAST (jingwen.comorbid.age AS INTEGER) >= 18
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='35-49'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) <= 49 and CAST (jingwen.comorbid.age AS INTEGER) >= 35
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='50+'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) >= 50
                                            ''')


    relabelComorbidQueryStringList.append('''
    UPDATE jingwen.comorbid
    SET age_categorical='0'
    WHERE CAST (jingwen.comorbid.age AS INTEGER) = 0
                                            ''')

    return relabelComorbidQueryStringList



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
                                            ''' + filterRace() + '''
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
                                            select jingwen.temp2.race, jingwen.temp2.sex, jingwen.temp2.age, jingwen.temp2.visit_type, jingwen.temp4.*
                                            from jingwen.temp4
                                            inner join jingwen.temp2
                                            on jingwen.temp4.id = jingwen.temp2.id and jingwen.temp4.siteid = jingwen.temp2.siteid
                                            and jingwen.temp2.age != NULL
                                            );
                                            '''
    addAgeCategoricalQueryString        =   '''
                                            ALTER TABLE jingwen.comorbid
                                            ADD age_categorical text NULL;
                                            '''



    print('[preProcessDB] Running queries. This might take a while ...')

    try:
        print('Filter race and join with typepatient ... ', end = " ")
        if pgIO.commitData(typePatientJoinQueryString , dbName = dbName):
            print('done\n')

        print('Remove duplicate visits ... ', end = " ")
        if pgIO.commitData(removeDuplicateVisitsQueryString , dbName = dbName):
            print('done\n')

        print('Join with pdiagnose [1/2] ... ', end = " ")
        if pgIO.commitData(pdiagnoseJoinQueryString , dbName = dbName):
            print('done\n')

        print('Join with pdiagnose [2/2] ... ', end = " ")
        if pgIO.commitData(pdiagnoseJoinQueryString2 , dbName = dbName):
            print('done\n')

        print('One hot diagnoses and SUD ... ', end = " ")
        if pgIO.commitData(oneHotDiagnosesQueryString , dbName = dbName):
            print('done\n')

        print('Join everything ... ', end = " ")
        if pgIO.commitData(joinEverythingQueryString , dbName = dbName):
            print('done\n')

        print('Relabelling')
        for relabelQuery in relabelComorbid():
            pgIO.commitData(relabelQuery , dbName = dbName)

        genRetrieve = pgIO.getDataIterator("select * from jingwen.comorbid", dbName = dbName, chunks = 100)
        dbColumnQueryString =       '''
                                    SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_schema = 'jingwen'
                                    AND table_name   = 'comorbid'
                                '''
        dbColumns = pgIO.getAllData(dbColumnQueryString, dbName = dbName)
        dbColumns = [item[0] for item in dbColumns]
        tempArray = [] #RUN THE QUERY
        for idx, data in enumerate(genRetrieve):
            tempArray += data
            print("Chunk: "+str(idx))
        
        rawData = pd.DataFrame(data = tempArray, columns = dbColumns)

        try: #SAVE THE PICKLE
            fileObjectSave = open(jsonConfig["outputs"]["intermediatePath"]+"db.pickle",'wb') 
            miscData = [SUDList, diagnosesList, rawSUDList, rawDiagnosesList]
            pickle.dump((miscData, rawData), fileObjectSave)   
            fileObjectSave.close()

        except Exception as e:
            logger.error(f'Issue saving to pickle: " {e}')

    except Exception as e:
        #   This probably won't print. Error is pgIO side, and I dont wanna modify
        #   it or write my own psycopg2 wrapper just to suppress error message.
        logger.error('Issue in query run. {}'.format(e))
        pass

    return




# explain
# select jingwen.temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
# from jingwen.temp2
# inner join raw_data.pdiagnose 
# on raw_data.pdiagnose.backgroundid = jingwen.temp2.id and raw_data.pdiagnose.siteid = jingwen.temp2.siteid
# limit 100

# EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS)

# EXPLAIN (COSTS, VERBOSE)
# create table jingwen.temp2_1 as(
# select jingwen.temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
# from jingwen.temp2
# inner join raw_data.pdiagnose 
# on raw_data.pdiagnose.backgroundid = jingwen.temp2.id and raw_data.pdiagnose.siteid = jingwen.temp2.siteid
# );

# select jingwen.temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
# from jingwen.temp2
# inner join raw_data.pdiagnose 
# on raw_data.pdiagnose.backgroundid = jingwen.temp2.id and raw_data.pdiagnose.siteid = jingwen.temp2.siteid

# explain
# WITH cte AS
#   (SELECT backgroundid
#    FROM raw_data.pdiagnose)
# SELECT jingwen.temp2.id
# FROM jingwen.temp2
# WHERE jingwen.temp2.id IN
#     (SELECT backgroundid
# FROM cte);

# explain

# WITH cte AS
#   (SELECT id
#    FROM jingwen.temp2)
# SELECT raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
# FROM raw_data.pdiagnose
# WHERE backgroundid 
# IN
#     (SELECT id
# FROM cte);

# SELECT "purchase"."id"
# FROM "purchase"
# INNER JOIN "user" ON ("purchase"."user_id" = "user"."id")
# WHERE "user"."account_id" IN
# (SELECT generate_series(1,1000));

# WITH user_ids AS
#   (SELECT id
#    FROM user
#    WHERE account_id IN
#        (SELECT generate_series(1,1000)))
# SELECT purchase.id
# FROM purchase
# WHERE user_id IN
#     (SELECT id
# FROM user_ids);







