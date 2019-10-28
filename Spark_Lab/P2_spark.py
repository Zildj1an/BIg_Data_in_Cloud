#!/usr/bin/python
#  Count URL Access Frequency
#  Using Spark (Data Flow Processing) algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)
#  Usage: $spark-submit P2_spark.py

from pyspark import SparkConf, SparkContext
import sys

# Change num threads with local[k]
conf = SparkConf().setMaster('local[*]').setAppName('URL_Access')
sc   = SparkContext(conf=conf)

RDD_source = sc.textFile("access_log")
words = RDD_source.flatMap(lambda x : x.split(' '))
the_word = words.filter(lambda x : "/" in x and "HTTP" not in x and "[" not in x)
result = the_word.map(lambda word: (word,1))
aggreg = result.reduceByKey(lambda a, b: a+b)
aggreg.saveAsTextFile("output_2")







