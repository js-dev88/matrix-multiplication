#!/usr/bin/env python3

import sys
from collections import defaultdict

dict_all = defaultdict(lambda: [])
list_l = {}
list_r = {}
dict_result = {}

#Combiner ?
for line in sys.stdin:
#Could be the combiner if we don't have to add the Matrix type into the current mapper.
	line = line.strip()
	#format de la ligne en entrée : i,k\tL,j,value
	row = line.split("\t")
	#row[0] = key = i,k
	#row[1] = value = L,j,Lij ou R,j,Rjk
	
	dict_all[row[0]].append(row[1])

	
f = open("/root/bigdata/test_keys_vals.txt", "a")
for key, val in dict_all.items():
	f.write('{key1}\t{val1}'.format(key1 = key, val1 = val))
	print('{key1}\t{val1}'.format(key1 = key, val1 = val))			
f.close()

#TESTED => OK



for key, key_values in dict_all.items():
	list_l = {}
	list_r = {}
	for value in key_values:
		split = value.split(",")
		if split[0] == "L":
			#split[1] => j
			#split[2] => Lij
			list_l[int(split[1])] = int(split[2])
		else: 
			#split[1] => j
			#split[2] => Rjk
			list_r[int(split[1])] = int(split[2])
	
	f = open("/root/bigdata/test_m_n_lists.txt", "a")
	f.write("\nListe L {key}\n".format(key = key))

	for i in list_l.values():
		f.write('{val1} \n'.format(val1 = i))	
	f.write("\nListe N {key}\n".format(key = key))

	#Multiply each L line element to each corresponding R column element
	for i in list_r.values():
		f.write('{val1} \n'.format(key1 = key, val1 = i))			
	f.write("\n===========================\n")
	f.close()
	
	element_result = 0 #somme des produits de ligne de L et colonne de R
	nb_j = max(len(list_l), len(list_r))
	for i in range(1, nb_j +1):
		try:
			element_result += list_l[i] * list_r[i]
		except KeyError:
			continue
	if element_result != 0:
		dict_result[key] = element_result


#Print the results
f = open("/root/bigdata/test_result.txt", "a")
for key, val in dict_result.items():
	f.write('{key1}\t{val1}\n'.format(key1 = key, val1 = val))
	print('{key1}\t{val1}\n'.format(key1 = key, val1 = val))
f.close()
