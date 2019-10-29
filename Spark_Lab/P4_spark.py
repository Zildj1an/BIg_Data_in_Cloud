#!/usr/bin/python
#  Movie Rating Data
#  Using Spark (Data Flow Processing) algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)
#  Usage: $spark-submit P4_spark.py

from pyspark import SparkConf, SparkContext
import sys,csv,os

# Change num threads with local[k]
# When you have one node the best way to go is local[*], but
# in a cluster, you need to remove the setMaster option and specify
# the number of worker nodes when submitting the code.
conf = SparkConf().setMaster('local[1]').setAppName('Movie_Rating')
sc   = SparkContext(conf=conf)

dataset     = sc.textFile("ratings.csv")
dataset.cache
header      = dataset.first()
# Apart from making it a RDD we remove the Header
RDD_source  = dataset.filter(lambda y: y != header).mapPartitions(lambda x: csv.reader(x))
RDD_source  = RDD_source.map(lambda m : (m[1],m[2]))

# Ranges
RDD_range1  = RDD_source.filter(lambda x: float(x[1]) <= float(1))
RDD_range2  = RDD_source.filter(lambda x: float(x[1]) <= float(2) and float(x[1]) > float(1))
RDD_range3  = RDD_source.filter(lambda x: float(x[1]) <= float(3) and float(x[1]) > float(2))
RDD_range4  = RDD_source.filter(lambda x: float(x[1]) <= float(4) and float(x[1]) > float(3))
RDD_range5  = RDD_source.filter(lambda x: float(x[1]) <= float(5) and float(x[1]) > float(4))

print("--------------------------------------------------")
print("The contents will be stored in the folder output_4")
print("in as many parts as logical cores available")
print("--------------------------------------------------")

RDD_range1.saveAsTextFile("output_4_RANGE1")
RDD_range2.saveAsTextFile("output_4_RANGE2")
RDD_range3.saveAsTextFile("output_4_RANGE3")
RDD_range4.saveAsTextFile("output_4_RANGE4")
RDD_range5.saveAsTextFile("output_4_RANGE5")
