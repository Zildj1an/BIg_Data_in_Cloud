#!/usr/bin/python
#  Distributed analysis of cinema data set (Map function A)
#  It contains 100004 ratings and 1296 tag applications across 9125 movies
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re, csv

header = False

for row in csv.reader(iter(sys.stdin.readline, '')):

    if header:
       movie_Id = row[1]
       rate = row[2]
       print movie_Id + "\t" + rate
    else:
        header = True


