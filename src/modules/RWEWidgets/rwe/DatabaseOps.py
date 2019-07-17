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
import scipy
from scipy.stats import chi2
from scipy.stats import chi2_contingency
import pickle
import math
import re
from tabulate import tabulate
import pandas as pd
import time
from lib.databaseIO import pgIO
from scipy.sparse import csc_matrix

from modules.RWEWidgets.rwe import Core as rwe

config     = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/RWEWidgets/widgets.json'))
logBase    = config['logging']['logBase'] + '.modules.RWEWidgets.rwe'
dbName     = jsonConfig["inputs"]["dbName"]

rawSchemaName = jsonConfig["inputs"]["rawSchemaName"]


def getAge(patientWindow = [1, 100000], timeWindow = [0, 10000]):

    query = '''
    select distinct on (patientid, days) age, patientid, days 
    from rwe_version1_1.typepatient
    where age is not null 
    and days >= {} and days <= {} 
    and patientid >= {} and patientid <= {}
    '''.format( timeWindow[0], timeWindow[1], patientWindow[0], patientWindow[1] )

    dataIterator = pgIO.getDataIterator(query, dbName=dbName, chunks=1000)

    dataBuffer = []

    for data in dataIterator:
        dataBuffer += data

    dataOut = pd.DataFrame(dataBuffer, columns=['age', 'cohort', 'days'])

    return rwe.Numeric('age', dataOut)




def getDiagnoses(patientWindow = [1, 100000], timeWindow = [0, 10000]):

    query = '''
    select array_agg(x.dsmno), patientid, days
    from 
    (
        select distinct on (dsmno, rwe_version1_1.pdiagnose.patientid, days) dsmno, rwe_version1_1.pdiagnose.typepatientid, rwe_version1_1.pdiagnose.patientid, days from 
        (
            select * from rwe_version1_1.typepatient 
                where
                days >= {} and days <= {} 
                and patientid >= {} and patientid <= {}
            ) as temp1
        inner join rwe_version1_1.pdiagnose
        on rwe_version1_1.pdiagnose.typepatientid = temp1.typepatientid      
    ) as x
    group by x.patientid, x.days
    '''.format( timeWindow[0], timeWindow[1], patientWindow[0], patientWindow[1] )

    dataIterator = pgIO.getDataIterator(query, dbName=dbName, chunks=1000)

    dataBuffer = []

    for data in dataIterator:
        dataBuffer += data

    dataOut = pd.DataFrame(dataBuffer, columns=['dsmno', 'cohort', 'days'])

    # Enums
    enumQuery = 'select * from rwe_version1_1_enums.pdiagnose_dsmno'
    enumList = pgIO.getAllData(enumQuery, dbName=dbName)
    enums = []
    for enum in enumList:
        enums += enum
    
    return rwe.CatList('dsmno', dataOut, enums)





def getRace(patientWindow = [1, 100000]):

    query = '''
    select distinct on (patientid) race, patientid
    from rwe_version1_1.background
    where patientid >= {} and patientid <= {}
    '''.format(patientWindow[0], patientWindow[1] )

    dataIterator = pgIO.getDataIterator(query, dbName=dbName, chunks=1000)

    dataBuffer = []

    for data in dataIterator:
        dataBuffer += data

    dataOut = pd.DataFrame(dataBuffer, columns=['race', 'cohort'])

    # Enums
    enumQuery = 'select * from rwe_version1_1_enums.background_race '
    enumList = pgIO.getAllData(enumQuery, dbName=dbName)
    enums = []
    for enum in enumList:
        enums += enum

    return rwe.Cat('race', dataOut, enums)





def getSex(patientWindow = [1, 100000]):

    query = '''
    select distinct on (patientid) sex, patientid
    from rwe_version1_1.background
    where patientid >= {} and patientid <= {}
    '''.format(patientWindow[0], patientWindow[1] )

    dataIterator = pgIO.getDataIterator(query, dbName=dbName, chunks=1000)

    dataBuffer = []

    for data in dataIterator:
        dataBuffer += data

    dataOut = pd.DataFrame(dataBuffer, columns=['sex', 'cohort'])

    # Enums
    enumQuery = 'select * from rwe_version1_1_enums.background_sex '
    enumList = pgIO.getAllData(enumQuery, dbName=dbName)
    enums = []
    for enum in enumList:
        enums += enum
    
    return rwe.Cat('sex', dataOut, enums)





def getVisitType(patientWindow = [1, 100000], timeWindow = [0, 10000]):

    query = '''
    select distinct on (visit_type, patientid, days) visit_type, patientid, days
    from rwe_version1_1.typepatient
    where
    days >= {} and days <= {} 
    and patientid >= {} and patientid <= {}
    '''.format( timeWindow[0], timeWindow[1], patientWindow[0], patientWindow[1] )

    dataIterator = pgIO.getDataIterator(query, dbName=dbName, chunks=1000)

    dataBuffer = []

    for data in dataIterator:
        dataBuffer += data

    dataOut = pd.DataFrame(dataBuffer, columns=['visit_type', 'cohort', 'days'])

    # Enums
    enumQuery = 'select * from rwe_version1_1_enums.typepatient_visit_type '
    enumList = pgIO.getAllData(enumQuery, dbName=dbName)
    enums = []
    for enum in enumList:
        enums += enum
    
    return rwe.Cat('visit_type', dataOut, enums)





def getMeds(patientWindow = [], timeWindow = []):

	query = '''
			select array_agg(x.medication), patientid, days
			from 
			(
			    select distinct on (medication, rwe_version1_1.meds.patientid, days) medication, rwe_version1_1.meds.typepatientid, rwe_version1_1.meds.patientid, days from 
				(
					select * from rwe_version1_1.typepatient
			'''

	rawData = []

	data = np.full((patientWindow[1]+1,timeWindow[1]+1), '', dtype=object)

	if patientWindow != [] and timeWindow != []:
		query += 'where days >= {} and days <= {} and patientid >= {} and patientid <= {}'.format(  timeWindow[0],
																									timeWindow[1],
																									patientWindow[0],
																									patientWindow[1]
																								  )

	else:
		print('No constraints on query. This is probably a bad idea, so query is aborted.')
		return

	query += '''
				 ) as temp1
				 inner join rwe_version1_1.meds
				 on rwe_version1_1.meds.typepatientid = temp1.typepatientid      
			 ) as x
			 group by x.patientid, x.days
			 '''

	rawData += pgIO.getAllData(query, dbName=dbName)

	for item in rawData:
		data[item[1]][item[2]] = item[0]

	np.savetxt("rweDB.getMeds.tsv", data, fmt='%s', delimiter="\u0009")

	return data





def getCGI_S(patientWindow = [], timeWindow = []):

	query = '''
			select distinct on (severity, rwe_version1_1.cgi.patientid, days) severity, rwe_version1_1.cgi.patientid, days 
            from 
			(
				select * from rwe_version1_1.typepatient
			'''

	rawData = []

	data = np.full((patientWindow[1]+1,timeWindow[1]+1), np.nan, dtype=float)

	if patientWindow != [] and timeWindow != []:
		query += 'where days >= {} and days <= {} and patientid >= {} and patientid <= {}'.format(  timeWindow[0],
																									timeWindow[1],
																									patientWindow[0],
																									patientWindow[1]
																								  )

	else:
		print('No constraints on query. This is probably a bad idea, so query is aborted.')
		return

	query += '''
			 ) as temp1
			 inner join rwe_version1_1.cgi
			 on rwe_version1_1.cgi.typepatientid = temp1.typepatientid   
			 '''

	rawData += pgIO.getAllData(query, dbName=dbName)

	for item in rawData:
		data[item[1]][item[2]] = item[0]

	np.savetxt("rweDB.getCGI_S.tsv", data, fmt='%f', delimiter="\u0009")

	return data


