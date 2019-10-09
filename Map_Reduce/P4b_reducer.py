#!/usr/bin/python
#  Distributed analysis of cinema data set (Reducer function B)
#  It contains 100004 ratings and 1296 tag applications across 9125 movies
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys

previous  = None
pos = 1

print "Range " + str(pos) + ":"

for line in sys.stdin:

    value, key = line.split('\t')

    if value != previous:
       if previous is not None:
           pos = pos + 1
           print "Range " + str(pos) + ":"

    sys.stdout.write(key)
    previous = value
