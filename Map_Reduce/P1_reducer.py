#!/usr/bin/python
#  Distributed version of the grep tool (Reduce function)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys

for line in sys.stdin:
    if line not in ['\n','\r\n']:
	sys.stdout.write(line)
