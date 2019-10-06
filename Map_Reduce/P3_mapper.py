#!/usr/bin/python
#  Distributed Stock Summary (Map function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import csv,sys

header = False

for row in csv.reader(iter(sys.stdin.readline, '')):
    if header:
       year = row[0][:4]
       val = row[4]
       if int(year) >= int("2009"):
       	  print year + '\t' + val
    else:
	header = True
