#!/usr/bin/python
#  Distributed analysis of NASA data set (Reduce function)
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

