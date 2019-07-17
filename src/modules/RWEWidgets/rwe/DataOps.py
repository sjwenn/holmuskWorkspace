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
from multipledispatch import dispatch
import numpy.ma as ma

from modules.RWEWidgets.rwe import Core as rwe

config     = jsonref.load(open('../config/config.json'))

jsonConfig = jsonref.load(open('../config/modules/RWEWidgets/widgets.json'))

logBase    = config['logging']['logBase'] + '.modules.RWEWidgets.rwe'
dbName     = jsonConfig["inputs"]["dbName"]

rawSchemaName = jsonConfig["inputs"]["rawSchemaName"]

def TR(data, operand, quantile=0.05):

    dataIn = data.df
    column = data.name

    if isinstance(data, rwe.Cat):

        dataOut = dataIn.drop(columns='days')

        dataOut = dataOut.groupby('cohort', sort=False, as_index=False).agg(lambda x: list(x.unique()))
        dataOut = dataOut.visit_type.str.join('|').str.get_dummies().join(dataOut[['cohort']])

        if len(operand[1]) > 0:
            dataOut = dataOut.drop(columns=operand[1])

        elif len(operand[0]) > 0:
            dataOut = dataOut.filter(operand[0] + ['cohort'])

        else:
            pass

        return rwe.Ohe(data.name, dataOut)  

    elif isinstance(data, rwe.Numeric):  

        if operand == 'max':
            dataOut = dataIn.groupby(['cohort'], sort=False, as_index=False)[column].max()

        elif operand == 'min':
            dataOut = dataIn.groupby(['cohort'], sort=False, as_index=False)[column].min()

        return rwe.Numeric(data.name, dataOut)  

    elif isinstance(data, rwe.CatList): 
        return  

    else:
        raise Exception('[Error] TR: Unsupported type ' + str(type(data)))
        return


def float2cat(data, filterPath, default=''):

    dataIn = data.df
    column = data.name

    if isinstance(data, rwe.Numeric):

        filter = pd.read_csv(filterPath, header=None)
        dataOut = dataIn.copy()
        dataOut[column].astype('str')
        dataOut[column] = default

        for idx,(lowerValue, upperValue, category) in filter.iterrows():
            dataOut.loc[( dataIn[column] <= upperValue) & ( dataIn[column] >= lowerValue), column] = category

        enums = filter[2].fillna(value=default).unique()

        return rwe.Cat(data.name, dataOut, enums)

    else:
        raise Exception('[Error] float2cat: Unsupported type ' + str(type(data)))
        return

def cat2cat(data, filterPath, default=''):

    dataIn = data.df
    column = data.name

    filter = pd.read_csv(filterPath, header=None)
    dataOut = dataIn.copy()

    if isinstance(data, rwe.Cat):

        dataOut[column] = default

        for idx,(value, category) in filter.iterrows():
            if category != '' and category == category: #test for nan, may be slow
                dataOut.loc[ dataIn[column] == value , column] = category
            else:
                dataOut.loc[ dataIn[column] == value , column] = default

        enums = filter[1].fillna(value=default).unique()
        return rwe.Cat(data.name, dataOut, enums)

    elif isinstance(data, rwe.CatList):

        # Parse list
        #dataIn[column] = dataIn[column].apply(lambda x: x.replace('{','').replace('}','').split(','))

        #### METHOD 1: LIST ####
        # for category, value in filterIterator.items():
        #     for index, dsmnoPerPerson in dataIn[column].items():
        #         if _listContainsItemInList(dsmnoPerPerson, value):
        #             dataIn[column+"_temp2"].index[index].append(category)

            # print("{}".format(category))
            
        # a = dataIn[column+"_temp"].to_numpy()
        # lens = [len(l) for l in a]
        # maxlen = max(lens)
        # arr = np.zeros((len(a),maxlen), str)
        # mask = np.arange(maxlen) < np.array(lens)[:,None]
        # arr[mask] = np.concatenate(a)

        #### METHOD 2: STRING ####
        filter           = pd.read_csv(filterPath, header=None)
        filterIterator   = filter.groupby(1)[0].apply(list)
        dataInStringList = dataIn[column].tolist()
        enumLength       = len(data.enums)

        for idx, value in enumerate(data.enums):

            print(str(idx).ljust(20)+"/"+str(enumLength))

            dataInStringList = np.char.replace(dataInStringList, '{', '')
            dataInStringList = np.char.replace(dataInStringList, '}', ',')

            search = filter[filter[0]==value]

            if len(search) > 0:
                search = search.iloc[0]
                dataInStringList = np.char.replace(dataInStringList, search[0] +",", search[1]+",")

            else:
                dataInStringList = np.char.replace(dataInStringList, value +"," , default+",")

        dataOut[column] = dataInStringList


        #### METHOD 3: MELT #### 
        # dataOut = dataIn[column].apply(pd.Series) \
        #         .merge(dataIn, right_index = True, left_index = True) \
        #         .melt(id_vars = ['cohort', 'days'], value_name = column) \
        #         .drop("variable", axis = 1) \
        #         .dropna()

        enums = filter[1].fillna(value=default).unique()
        return rwe.CatList(data.name, dataOut, enums)

    else:
        raise Exception('[Error] cat2cat: Unsupported type ' + str(type(data)))
        return


def cat2bool(data, conditions):

    dataIn = data.df
    column = data.name

    if isinstance(data, rwe.Cat):

        dataOut = dataIn.copy()
        dataOut[column].astype('bool')

        dataOut['tempContains'] = True
        dataOut['tempNotContains'] = True

        for value in conditions[0]:
            dataOut.loc[ dataIn[column] != value , 'tempContains'] = False

        for value in conditions[1]:
            dataOut.loc[ dataIn[column] == value , 'tempNotContains'] = False

        dataOut[column] = dataOut['tempContains'] & dataOut['tempNotContains']

        dataOut = dataOut.drop(columns=['tempContains','tempNotContains'])

        return rwe.Ohe(data.name, dataOut)

    else:
        raise Exception('[Error] cat2bool: Unsupported type ' + str(type(data)))
        return


def cat2ohe(data):

    dataIn = data.df
    column = data.name

    if isinstance(data, rwe.Cat):

        dataOut = pd.get_dummies(dataIn[column]).join(dataIn[['cohort']])

        return rwe.Ohe(data.name, dataOut)

    else:
        raise Exception('[Error] cat2ohe: Unsupported type ' + str(type(data)))
        return

def oheDrop(data, toDrop):

    dataIn = data.df

    if isinstance(data, rwe.Ohe):

        dataOut = dataIn.drop(columns=toDrop)

        data.df = dataOut

        return data

    else:
        raise Exception('[Error] oheDrop: Unsupported type ' + str(type(data)))
        return


def OR(data, column):

    # Check if all data has the same legnth
    numRows = data[0].df.shape[0]
    for item in data:
        if numRows != item.df.shape[0]:
            raise Exception('[Error] OR: Inputs do not have the same number of rows')
            return

    # Check if number of columns matches with data
    if len(data) != len(column):
        raise Exception('[Error] OR: Number of columns and data do not match')
        return



    dataOut = data[0].df
    dataOut['or_result'] = 0
    dataOut.loc[np.logical_or(data[0].df[column[0]], data[1].df[column[1]]), 'or_result'] = 1

    data[0].df = dataOut[['or_result', 'cohort']]

    return data[0]


# def _listContainsItemInList(list1, list2): 
#     # Errrrm
#     for m in list1: 
#         for n in list2: 
#             if m == n: 
#                 return True                
#     return False 