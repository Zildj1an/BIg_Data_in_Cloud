#!/usr/bin/python
#  Distributed version of the grep tool
#  Using Spark (Data Flow Processing) algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)
#  Usage: $spark-submit P1_spark.py word

from pyspark import SparkConf, SparkContext
import sys

# Change num threads with local[k]
conf = SparkConf().setMaster('local[*]').setAppName('Distributed_Grep')
sc   = SparkContext(conf=conf)

searched_word = sys.argv[1]

RDD_source = sc.textFile("input.txt")
words = RDD_source.flatMap(lambda x : x.split('\n'))
the_word = words.filter(lambda x : searched_word in x)
the_word.saveAsTextFile("output_1")

