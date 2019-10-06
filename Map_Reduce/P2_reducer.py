#!/usr/bin/python
#  URL Access Frequency tool (Reduce function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

previous = None
sum = 0

for line in sys.stdin:
    key, value = line.split('\t')

    if key != previous:
       if previous is not None:
           print previous + '\t' + str(sum)
       previous = key
       sum = 0

    sum = sum + int(value)

if previous is not None:
	print previous + '\t' + str(sum)
