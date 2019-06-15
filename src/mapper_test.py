#!/usr/bin/env python3
import sys
import os

#Récupération du type de la matrice
filepath = os.environ["map_input_file"] 
filename = os.path.split(filepath)[-1]
type_matrix = filename.split('_')[0]

# lecture STDIN
for line in sys.stdin:
    # split the line into matrix element
    element = line.split('\t')
    #The mapper is here used to add the matrix type into the input lines : from i	j	val to L	i	j	val
    f = open("/root/bigdata/test.txt", "a")
    f.write('{type}\t{line}\t{column}\t{value}'.format(type = type_matrix, line = element[0], column = element[1], value = element[2].rstrip()))
    print('{type}\t{line}\t{column}\t{value}'.format(type = type_matrix, line = element[0], column = element[1], value = element[2].rstrip()))