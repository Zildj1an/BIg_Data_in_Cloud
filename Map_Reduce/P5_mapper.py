#!/usr/bin/python
#  Distributed analysis of NASA data set (Map function A)
#  Using Map-Reduce Cloud Batch algorithm.
#  Author: Carlos Bilbao (github.com/Zildj1an)

import sys, re, csv

for row in csv.reader(iter(sys.stdin.readline, '')):

       meteo_Id = row[3]
       mass = row[4]
       meteo_Id = meteo_Id.split(' ')[0]

       if meteo_Id[len(meteo_Id) - 1] == ',': # Some cleanning
	   meteo_Id = meteo_Id[:len(meteo_Id) - 1]

       if mass and (not mass.isspace()):
	       print meteo_Id + "\t" + mass

