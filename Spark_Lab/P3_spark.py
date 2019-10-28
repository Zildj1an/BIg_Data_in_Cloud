#!/usr/bin/python
#  Stock Summary
#  Using Spark (Data Flow Processing) algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)
#  Usage: $spark-submit P3_spark.py

from pyspark import SparkConf, SparkContext
import sys,csv

# Change num threads with local[k]
conf = SparkConf().setMaster('local[1]').setAppName('Stock_Summary')
sc   = SparkContext(conf=conf)

dataset     = sc.textFile("GOOGLE.csv")
header      = dataset.first()
# Apart from making it a RDD we remove the Header
RDD_source  = dataset.filter(lambda y: y != header).mapPartitions(lambda x: csv.reader(x))
RDD_source  = RDD_source.map(lambda m : (m[0][:4],(float(m[4]),1))).filter(lambda x: int(x[0]) > int('2008'))
aggreg = RDD_source.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).map(lambda x : (x[0],x[1][0]/x[1][1]))
aggreg.saveAsTextFile("output_3")


