#!/usr/bin/python
#  Distributed analysis of cinema data set (Reduce function A)
#  It contains 100004 ratings and 1296 tag applications across 9125 movies
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

previous = None
elems    = 0
sum      = 0

for line in sys.stdin:
    key, value = line.split('\t')

    if key != previous:
       if previous is not None:
           print previous + '\t' + str(sum/elems)
       previous = key
       sum = 0
       elems = 1

    sum = sum + float(value)
    elems = elems + 1

if previous is not None:
        print previous + '\t' + str(sum/elems)



