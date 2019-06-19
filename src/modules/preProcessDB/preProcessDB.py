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

    raceFilter = pd.read_csv(jsonConfig["inputs"]["raceFilterPath"])
    filterRaceQueryString = '''
                            select id, siteid, race, sex from raw_data.background 
                            where (ethnicity not ilike 'hisp%') and (
                            '''

    # Pick out relavant races
    for idx, (race, category) in raceFilter.iterrows():
        if category == category: #not NAN
            filterRaceQueryString += "(race='" + race + "')or"
    # Remove last 'or'
    filterRaceQueryString = filterRaceQueryString[:-2] + ")"

    return filterRaceQueryString



@lD.log(logBase + '.main')
def main(logger, resultsDict):

    typePatientJoinQueryString      =   '''
                                        select background.id, background.siteid, background.race, background.sex, 
                                        raw_data.typepatient.age, raw_data.typepatient.visit_type, raw_data.typepatient.created
                                        from
                                        (
                                        ''' + filterRace() + '''
                                        ) as background
                                        inner join raw_data.typepatient 
                                        on raw_data.typepatient.backgroundid = background.id and raw_data.typepatient.siteid = background.siteid
                                        '''

    cteQueryString                  =   '''
                                        ;with cteRemoveDuplicate AS
                                        (
                                        select *,
                                        ROW_NUMBER() OVER (PARTITION BY id, siteid ORDER BY created DESC) AS rn
                                        from (
                                             ''' + typePatientJoinQueryString + '''
                                        ) as temp1
                                        )
                                        '''

    removeDuplicateVisitsQueryString =  '''
                                        select id, siteid, race, sex, age, visit_type
                                        from cteRemoveDuplicate
                                        where rn = 1
                                        '''

    pdiagnoseJoinQueryString         =  '''
                                        select temp2.*, raw_data.pdiagnose.dsmno, raw_data.pdiagnose.diagnosis
                                        from
                                        (
                                        ''' + removeDuplicateVisitsQueryString + '''
                                        ) as temp2
                                        inner join raw_data.pdiagnose 
                                        on raw_data.pdiagnose.backgroundid = temp2.id and raw_data.pdiagnose.siteid = temp2.siteid
                                        '''    
                                                                        
    finalQueryString = cteQueryString + pdiagnoseJoinQueryString  
                                    
    print('[preProcessDB] Running query. This might take a while ...')
    #val = pgIO.getAllData( finalQueryString , dbName = dbName)
    print(finalQueryString)

    return




