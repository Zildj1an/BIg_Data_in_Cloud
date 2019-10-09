#!/usr/bin/python
#  Distributed analysis of cinema data set (Map function B)
#  It contains 100004 ratings and 1296 tag applications across 9125 movies
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

for line in sys.stdin:

	key, value = line.split('\t')

        value = float(value[:len(value) -1])

	if value <= 1:
           print "1 \t" + key
        elif value <= 2:
           print "2 \t" + key
        elif value <= 3:
	   print "3 \t" + key
        elif value <= 4:
           print "4 \t" + key
        elif value <= 5:
	   print "5 \t" + key
	# A exception here could be interesting
