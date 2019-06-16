#!/usr/bin/env python3
import sys
import os

#Récupération du type de la matrice
filepath = os.environ["map_input_file"] 
filename = os.path.split(filepath)[-1]
type_matrix = filename.split('_')[0]

f = open("/root/bigdata/mapper_0.txt", "a")
# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into matrix element
    element = line.split('\t')
    # we want pair as ((i, j), (L, v)) for the first matrix, ((j, k), (R, v)) for the second matrix
	
	#(i	L, j, v)
	#(k	R, j, v)
	# we want pair as ((i, j), (L, v)) for the first matrix, ((j, k), (R, v)) for the second matrix
    if type_matrix == 'L':
        print('{},{}\t{},{}'.format(element[0], element[1], 'L', element[2]))
    if type_matrix == 'R':
        print('{},{}\t{},{}'.format(element[1], element[0], 'R', element[2]))

    
