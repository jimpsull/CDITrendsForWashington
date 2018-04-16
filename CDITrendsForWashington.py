# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 13:06:32 2018

@author: SulliJP
"""

#The HW assignment for week 5 is below.  It didn't specify which data set
#So I wanted to find a segment of the CDI data that might be interesting
#I started looking at the whole dataset but the computer froze
#So I started with just Washington

#Create a new Python script that includes the following:
#Import statements
#Loading your dataset
#Histogram of numeric variable
#Median imputation of a numeric variable
#Outlier replacement if applicable
#Comments explaining the code blocks
#Summary comment block on how the numeric variables have been treated: 
#    which ones had outliers, required imputation, 
#    distribution, removal of rows/columns.

import numpy as np
import pandas as pd
import sklearn as skl

#https://stackoverflow.com/questions/2887878/
#   importing-a-csv-file-into-a-sqlite3-database-table-using-python

import csv
import sqlite3

con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("CREATE TABLE cdi (YearStart int, DataValueType text, Topic text, \
                               Question text, \
                               DataValue real, DataValueUnit text);")
#with open('C:/tableau/US_CDI.csv') as fin:
with open('C:/tableau/WA_Overall_CDI.csv') as fin:
    dr = csv.DictReader(fin)
    to_db=[(i['YearStart'], i['DataValueType'], i['Topic'], i['Question'], \
           i['DataValue'], i['DataValueUnit']) for i in dr]
    
cur.executemany("INSERT INTO cdi (YearStart, DataValueType, Topic, Question,\
               DataValue, DataValueUnit) VALUES (?, ?, ?, ?, ?, ?);", to_db)
con.commit()

cur.execute("SELECT \
            cdi.Topic, cdi.Question, cdi.DataValueType,\
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart <= 2012) as StartValue, \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) as MidValue, \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2015) as EndValue \
            \
            FROM cdi\
            WHERE \
            DataValue is not null AND \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2015) > \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) AND \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) > \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart <= 2012) \
            Group by Topic, Question, DataValueType")
    
print("Washington State Chronic Disease Indicators Trending Up:")
TrendingUp = cur.fetchall()
for r in TrendingUp:
    print(r)

cur.execute("SELECT \
            cdi.Topic, cdi.Question, cdi.DataValueType,\
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart <= 2012) as StartValue, \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) as MidValue, \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2015) as EndValue \
            \
            FROM cdi WHERE \
            DataValue is not null AND \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2015) < \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) AND \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart >= 2013 AND \
            YearStart <= 2014) < \
            (select avg(DataValue) from cdi ccddii \
            where ccddii.Topic = cdi.Topic AND ccddii.Question = \
            cdi.Question and cdi.DataValueType = ccddii.DataValueType \
            AND YearStart <= 2012) \
            Group by Topic, Question, DataValueType")
    
print("Washington State Chronic Disease Indicators Trending Down:")
TrendingDown = cur.fetchall()
for r in TrendingDown:
    print(r)
    
con.close()
    
    
#filepath = 'C:/tableau/US_CDI.csv'
#CDI = pd.read_csv(filepath, dtype = object)

#UniqueTopics = CDI.Topic.unique()

#UniqueQuestions = CDI.Question.unique()
#print(UniqueQuestions)