#!/usr/bin/python
#  Meteorite Landing
#  Using Spark (Data Flow Processing) algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)
#  Usage: $spark-submit P5_spark.py

from pyspark import *
#SparkConf, SparkContext
import sys,csv

# Change num threads with local[k]
conf = SparkConf().setMaster('local[*]').setAppName('Meteorite_Landing')
sc   = SparkContext(conf=conf)

dataset = sc.textFile("Meteorite_Landings.csv")
dataset.cache
# Apart from making it a RDD we remove the Header
RDD_source  = dataset.mapPartitions(lambda x: csv.reader(x).split('\n'))

def is_int(y):
   try:
     int_y = int(y)
     return True
   except:
     return False

RDD_source  = RDD_source.filter(lambda m: len(m) > 4 and is_int(m[4])).flatMap(lambda m: (m[3],int(m[4])))
aggreg      = RDD_source.reduceByKey(lambda a,b: int(a)+int(b))

print("--------------------------------------------------")
print("The contents will be stored in the folder output_5")
print("in as many parts as logical cores available")
print("--------------------------------------------------")

aggreg.saveAsTextFile("output_5")
