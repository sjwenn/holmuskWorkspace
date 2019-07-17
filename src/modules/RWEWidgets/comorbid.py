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
from modules.RWEWidgets.rwe import Core        as rwe
from modules.RWEWidgets.rwe import DatabaseOps as rweDB
from modules.RWEWidgets.rwe import DataOps     as rweData
from modules.RWEWidgets.rwe import StatOps     as rweStat
import psycopg2 
from scipy.sparse import csc_matrix

import random
from collections import defaultdict
from heapq import nlargest

from luigi import six
import luigi.contrib.postgres
import luigi

config     = jsonref.load(open('../config/config.json'))
jsonConfig = jsonref.load(open('../config/modules/RWEWidgets/widgets.json'))
logBase    = config['logging']['logBase'] + '.modules.RWEWidgets.widgets'
dbName     = jsonConfig["inputs"]["dbName"]

rawSchemaName = jsonConfig["inputs"]["rawSchemaName"]

class TimeTaskMixin(object):
    '''
    A mixin that when added to a luigi task, will print out
    the tasks execution time to standard out, when the task is
    finished
    '''
    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):
        print('### PROCESSING TIME ###: ' + str(processing_time) + '\n')
















 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  w00xx                   #
 ############################################


class w0000(rwe.SourceWidget):

    def data(self):
        dataOut = rweDB.getRace()
        return dataOut


class w0001(rwe.ProcessWidget):

    def requires(self):
        return w0000()

    def data(self, dataIn):
        dataOut = rweData.cat2cat(dataIn, '../data/raw_data/RWEWidgets/Filters/Race_Filter.csv', default='other')
        return dataOut


class w0002(rwe.ProcessWidget):

    def requires(self):
        return w0001()

    def data(self, dataIn):
        dataOut = rweData.cat2ohe(dataIn)
        return dataOut


class w0003(rwe.ProcessWidget):

    def requires(self):
        return w0002()
        
    def data(self, dataIn):
        dataOut = rweData.oheDrop(dataIn, 'other')
        return dataOut


class w0004(rwe.ProcessWidget):

    def requires(self):
        return w0001()

    def data(self, dataIn):
        dataOut = rweData.cat2bool(dataIn, [['AA'],[]])
        return dataOut


class w0005(rwe.ProcessWidget):

    def requires(self):
        return w0001()
        
    def data(self, dataIn):
        dataOut = rweData.cat2bool(dataIn, [['NHPI'],[]])
        return dataOut


class w0006(rwe.ProcessWidget):

    def requires(self):
        return w0001()

    def data(self, dataIn):
        dataOut = rweData.cat2bool(dataIn, [['NHPI'],[]])
        return dataOut


class w0007(rwe.ProcessWidget):

    def requires(self):
        return w0001()

    def data(self, dataIn):
        dataOut = rweData.cat2bool(dataIn, [[],['other']])
        return dataOut











 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  w01xx                   #
 ############################################
 

class w0100(rwe.SourceWidget):

    def data(self):
        dataOut = rweDB.getAge()
        return dataOut


class w0101(rwe.ProcessWidget):

    def requires(self):
        return w0100()

    def data(self, dataIn):
        dataOut = rweData.TR(dataIn, 'min')
        return dataOut


class w0102(rwe.ProcessWidget):

    def requires(self):
        return w0101()

    def data(self, dataIn):
        dataOut = rweData.float2cat(dataIn, '../data/raw_data/RWEWidgets/Filters/Age_Filter.csv')
        return dataOut


class w0103(rwe.ProcessWidget):

    def requires(self):
        return w0102()

    def data(self, dataIn):
        dataOut = rweData.cat2ohe(dataIn)
        return dataOut











 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  w02xx                   #
 ############################################


class w0200(rwe.SourceWidget):

    def data(self):
        dataOut = rweDB.getSex()
        return dataOut



class w0201(rwe.ProcessWidget):

    def requires(self):
        return w0200()
        
    def data(self, dataIn):
        dataOut = rweData.cat2ohe(dataIn)
        return dataOut











 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  w03xx                   #
 ############################################


class w0300(rwe.SourceWidget):

    def data(self):
        dataOut = rweDB.getVisitType()
        return dataOut


class w0301(rwe.ProcessWidget):

    def requires(self):
        return w0300()

    def data(self, dataIn):
        dataOut = rweData.cat2cat(dataIn, '../data/raw_data/RWEWidgets/Filters/Setting_Filter.csv', default='other')
        return dataOut


class w0302(rwe.ProcessWidget):

    def requires(self):
        return w0301()

    def data(self, dataIn):
        dataOut = rweData.TR(dataIn, [['Hospital'],[]])
        return dataOut


class w0303(rwe.ProcessWidget):

    def requires(self):
        return w0301()

    def data(self, dataIn):
        dataOut = rweData.TR(dataIn, [[],['Hospital']])
        return dataOut


class w0304(rwe.ProcessWidget):

    def requires(self):
        return [w0302(), w0303()]

    def data(self, dataIn):
        dataOut = rweData.OR(dataIn, ['Hospital', 'Mental health center'])
        return dataOut









 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  w04xx                   #
 ############################################


class w0400(rwe.SourceWidget):
    
    def data(self):
        dataOut = rweDB.getDiagnoses()
        return dataOut


# class w0401(rwe.ProcessWidget):

#     def requires(self):
#         return w0400()

#     def data(self, dataIn):
#         dataOut = rweData.cat2cat(dataIn, '../data/raw_data/RWEWidgets/Filters/Diganoses_Filter.csv', default='other')
#         return dataOut


# class w0402(rwe.ProcessWidget):

#     def requires(self):
#         return w0401()

#     def data(self, dataIn):
#         dataOut = TR
#         return dataOut


# class w0403(rwe.ProcessWidget):

#     def requires(self):
#         return w0400()

#     def data(self, dataIn):
#         dataOut = rweData.cat2cat(dataIn, '../data/raw_data/RWEWidgets/Filters/SUD_Filter.csv', default='other')
#         return dataOut


# class w0404(rwe.ProcessWidget):

#     def requires(self):
#         return w0403()

#     def data(self, dataIn):
#         dataOut = TR
#         return dataOut









 ############################################
 #  ____  ____      _    _   _  ____ _   _  #
 # | __ )|  _ \    / \  | \ | |/ ___| | | | #
 # |  _ \| |_) |  / _ \ |  \| | |   | |_| | #
 # | |_) |  _ <  / ___ \| |\  | |___|  _  | #
 # |____/|_| \_\/_/   \_\_| \_|\____|_| |_| #
 #                                          #
 #                  outxx                   #
 ############################################


class out00(rwe.SinkWidget):
    
    def requires(self):
        return [w0007(), w0003(), w0103()]

    def data(self, dataIn):
        dataOut = rweStat.Chi2(dataIn[0], dataIn[1], dataIn[2])
        return dataOut





       
















@lD.log(logBase + '.main')
def main(logger, resultsDict):

    luigi.build([out00()], local_scheduler=True)

    return

