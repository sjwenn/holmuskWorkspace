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

@lD.log(logBase + '.filterRace')
def filterRace(logger):

    # Prepare filter sring
    raceFilter = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])
    filterRaceQueryString = '''
                            select id, siteid, race, sex from raw_data.background 
                            where (ethnicity not ilike 'hisp%') and (
                            '''

    # Create filter string
    for idx, (race, category) in raceFilter.iterrows():
        if category == category: #not NAN
            filterRaceQueryString += "(race='" + race + "')or"

    # Remove last 'or'
    filterRaceQueryString = filterRaceQueryString[:-2] + ")"

    return filterRaceQueryString



@lD.log(logBase + '.main')
def main(logger, resultsDict):

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
                                            create table jingwen.temp3 as(
                                            select distinct on (jingwen.temp2.id, jingwen.temp2.siteid, raw_data.pdiagnose.dsmno)
                                            jingwen.temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
                                            from jingwen.temp2
                                            inner join raw_data.pdiagnose 
                                            on raw_data.pdiagnose.backgroundid = jingwen.temp2.id and raw_data.pdiagnose.siteid = jingwen.temp2.siteid
                                            );
                                            '''


    print('[preProcessDB] Running queries. This might take a while ...')

    print('Filter race and join with typepatient ... ', end = " ")
    if pgIO.commitData(typePatientJoinQueryString , dbName = dbName):
        print('done')

    print('Remove duplicate visits ... ', end = " ")
    if pgIO.commitData(removeDuplicateVisitsQueryString , dbName = dbName):
        print('done')

    print('Join with pdiagnose ... ', end = " ")
    if pgIO.commitData(pdiagnoseJoinQueryString , dbName = dbName):
        print('done')




    #pgIO.getDataIterator( removeDuplicateVisitsQueryString , dbName = dbName, chunks = 1000)

    # tempArray = []
    # for idx, data in enumerate(genRetrieve):
    #     tempArray.append(data)
    #     print("Chunk: " + str(idx))

    #cols = ['id','siteid','race','sex','age_numeric','visit_type','age', 'dsm', 'diagnosis']
    #df = pd.DataFrame(data = tempArray[0], columns = cols)

    #print(df.head(100))

    return




