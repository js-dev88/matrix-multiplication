#!/usr/bin/env python3
import sys

def keep_matching_index(elem_L, elem_R):
    R_index = [x[1] for x in elem_R]
    for el in elem_L:
        if el[1] not in R_index:
            elem_L.remove(el)

    L_index = [x[1] for x in elem_L]
    for el in elem_R:
        if el[1] not in L_index:
            elem_R.remove(el)
    
    return elem_L, elem_R

def join(super_L, elements_R):
    for elem_L in super_L:
        for elem_R in super_R:
            if len(elem_L) > 0 and len(elem_R) > 0:
                elem_L, elem_R = keep_matching_index(elem_L, elem_R)
                print('{}\t{}'.format(elem_L, elem_R))
 
#on fabrique les lignes et colonnes
current_index = 0
index = 0
elements_L = []
super_L = []

elements_R = []
super_R = []
first_line = True


# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split key , value
    elements = line.split('\t')
    keys = elements[0].split(',')
    values = elements[1].split(',')

    index = int(keys[0])
    second_index = int(keys[1])
    type_mat = values[0]
    value = int(values[1])

    if current_index != index:
        if not first_line:
            toto = 1
            #join(super_L, elements_R)
            super_L.append(elements_L)
            super_R.append(elements_R)
            elements_L = []
            elements_R = []
        else:
            first_line = False

        current_index = index

    if current_index == index:
        if type_mat == 'L':
            elements_L.append((index, second_index, value))
        else:
            elements_R.append((index, second_index, value))


super_L.append(elements_L)
super_R.append(elements_R)
join(super_L, super_R)

