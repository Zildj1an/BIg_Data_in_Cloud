#!/usr/bin/python
#  URL Access Frequency tool (Map function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re

for line in sys.stdin:

    line  = re.sub(r'^\W+|\W+$','', line)
    words = re.findall(r'(https?://\S+)', line)

    for word in words:
        print word + "\t1"


