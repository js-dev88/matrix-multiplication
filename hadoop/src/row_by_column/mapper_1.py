#!/usr/bin/env python3
import sys

# lecture STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()
	# split key , value
	elements = line.split('\t')
	liste_L = eval(elements[0])
	liste_R = eval(elements[1])
	key = liste_L[0][0] #ligne de L
	secondary_key = liste_R[0][0]
	liste_result = []
	#max_size = max(len(liste_L),len(liste_R))
	for i, elem_L in enumerate(liste_L):
		mul =  (int(elem_L[2]) * int(liste_R[i][2]))
		liste_result.append(mul)
	print('{},{}\t{}'.format(key,secondary_key,liste_result))