#!/usr/bin/python
#  Distributed version of the grep tool (Map function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

for line in sys.stdin:

    line  = re.sub(r'^\W+|\W+$','', line)
    words = re.split(r"\W+", line)

    for word in words:
        if word in sys.argv:
            print(line)
	    break
