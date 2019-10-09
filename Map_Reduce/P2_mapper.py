#!/usr/bin/python
#  URL Access Frequency tool (Map function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

the_url = None

for line in sys.stdin:

    line  = re.sub(r'^\W+|\W+$','', line)

    get = False
    for word in line.split(' '):
        if get == True:
	   the_url = word
	   break
	if word == '"GET':
	   get = True

    if the_url is not None:
	print the_url + "\t1"
	the_url = None

