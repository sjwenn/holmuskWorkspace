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

from luigi import six
import luigi.contrib.postgres
import luigi

config     = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/RWEWidgets/widgets.json'))
logBase    = config['logging']['logBase'] + '.modules.RWEWidgets.rwe'
dbName     = jsonConfig["inputs"]["dbName"]

rawSchemaName = jsonConfig["inputs"]["rawSchemaName"]

# 



class SourceWidget(luigi.Task):

    def data(self):
        return 'none'

    def run(self):
        dataOut = self.data()

        with open(self.output().path, 'wb') as output:
            dataOut.df.to_csv("../data/intermediate/RWEWidgets/dumps/{}:{}.tsv".format(self.__class__.__name__, dataOut.__class__.__name__), sep='\t', index=False)
            pickle.dump(dataOut, output, protocol=pickle.HIGHEST_PROTOCOL)
                
    def output(self):
        return luigi.LocalTarget("../data/intermediate/RWEWidgets/{}.pickle".format(self.__class__.__name__))



class ProcessWidget(luigi.Task):

    @property
    def dataIn(self):

        if isinstance(self.input(), list):
            dataIn = []
            for inputFile in self.input():
                with open(inputFile.path, 'rb') as input:
                    dataIn.append(pickle.load(input))
            return dataIn

        else:
            with open(self.input().path, 'rb') as input:
                dataIn = pickle.load(input)
            return dataIn

    def data(self, dataIn):
        return 'none' # To be overriden

    def run(self):

        dataOut = self.data(self.dataIn)
        
        with open(self.output().path, 'wb') as output:
            dataOut.df.to_csv("../data/intermediate/RWEWidgets/dumps/{}:{}.tsv".format(self.__class__.__name__, dataOut.__class__.__name__), sep='\t', index=False)
            pickle.dump(dataOut, output, protocol=pickle.HIGHEST_PROTOCOL)

    def output(self):
        return luigi.LocalTarget("../data/intermediate/RWEWidgets/{}.pickle".format(self.__class__.__name__))



class SinkWidget(luigi.Task):

    @property
    def dataIn(self):

        if isinstance(self.input(), list):
            dataIn = []
            for inputFile in self.input():
                with open(inputFile.path, 'rb') as input:
                    dataIn.append(pickle.load(input))
            return dataIn

        else:
            with open(self.input().path, 'rb') as input:
                dataIn = pickle.load(input)
            return dataIn

    def data(self, dataIn):
        return 'none' # To be overriden

    def run(self):

        dataOut = self.data(self.dataIn)
        print('*'*40 + self.__class__.__name__)
        print(dataOut)
        print('*'*40+"\n"*4)

    def output(self):
        return luigi.LocalTarget("../data/intermediate/RWEWidgets/{}.pickle".format(self.__class__.__name__))






class BaseType:
    
    def __init__(self, name, df):
        self.name    = name
        self.df      = df

    @property
    def hasCohort(self):
        return 'cohort' in self.df

    @property
    def hasTime(self):
        return 'days' in self.df


class Cat(BaseType):
    
    def __init__(self, name, df, enums):
        self.name    = name
        self.df      = df
        self.enums   = enums


class CatList(BaseType):
    
    def __init__(self, name, df, enums):
        self.name    = name
        self.df      = df
        self.enums   = enums


class Ohe(BaseType):
    
    def __init__(self, name, df):
        self.name    = name
        self.df      = df


class Numeric(BaseType):
    
    def __init__(self, name, df):
        self.name    = name
        self.df      = df


class NumericList(BaseType):
    
    def __init__(self, name, df):
        self.name    = name
        self.df      = df


class Bool(BaseType):
    
    def __init__(self, name, df):
        self.name    = name
        self.df      = df












# class day:

#     def __init__(self, day):
#         if isinstance(day, (int, float)):
#             self.day = day
#         else:
#             raise TypeError('Type "day" only accepts int or float.')

#     def __str__(self):
#         return str(self.day) 

#     def __int__(self):
#         return self.day

#     def __eq__(self, other):
#         return self.day == other

#     def __lt__(self, other):
#         return self.day < other

#     def __le__(self, other):
#         return self.day <= other

#     def __gt__(self, other):
#         return self.day > other

#     def __ge__(self, other):
#         return self.day >= other

#     def __add__(self, other):
#         self.day = self.day + other
#         return self 

#     def __sub__(self, other):
#         self.day = self.day - other
#         return self 

#     def __mul__(self, other):
#         self.day = self.day * other
#         return self 

#     def __truediv__(self, other):
#         self.day = self.day / other
#         return self 

#     def value(self):
#         return float(self.day)


# class day_range:

#     def __init__(self, dayLower, dayUpper):
#         if isinstance(dayLower, day) and isinstance(dayUpper, day):
#             self.day_range =  [False] * int(dayLower)
#             self.day_range += [True] * int(dayUpper)
#         else:
#             raise TypeError('Type "day-range" only accepts day.')

#     def __str__(self):
#         return str(self.day_range)

#     def __len__(self):
#         return len(self.day_range)

#     def pad(self, targetLength):
#         return self.day_range + [False] * (targetLength - len(self.day_range))

#     def value(self):
#         return self.day_range

#     def mask(self, targetLength = -1):

#         if targetLength == -1:
#             return np.invert(self.day_range)

#         elif targetLength > 0:
#             return np.invert(self.day_range + [False] * (targetLength - len(self.day_range)))






