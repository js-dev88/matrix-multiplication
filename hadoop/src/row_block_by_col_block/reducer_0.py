#!/usr/bin/env python3
import sys
chunk_length = 2

def keep_matching_index(elem_L, elem_R):
    R_index = [x[1] for x in elem_R]
    tmp_elem_L = elem_L.copy()
    for el in elem_L:
        if el[1] not in R_index: 
            tmp_elem_L.remove(el) 

    L_index = [x[1] for x in elem_L]
    tmp_elem_R = elem_R.copy()
    for el in elem_R:
        if el[1] not in L_index:
            tmp_elem_R.remove(el)
    
    return tmp_elem_L, tmp_elem_R, elem_L, elem_R

def divide_in_chunks(chunk_length, elem_L, elem_R):
    if len(elem_L) > 0 and len(elem_R) > 0:
        elem_L_chunked = [elem_L[i:i+chunk_length] for i in range(0, len(elem_L), chunk_length)]
        elem_R_chunked = [elem_R[i:i+chunk_length] for i in range(0, len(elem_R), chunk_length)]
        for i, chunk_list_L in enumerate(elem_L_chunked):
            print('{}\t{}'.format(chunk_list_L, elem_R_chunked[i]))

def join(super_L, elements_R):
    for elem_L in super_L:
        for elem_R in super_R:
            if len(elem_L) > 0 and len(elem_R) > 0:
                tmp_elem_L, tmp_elem_R, elem_L, elem_R = keep_matching_index(elem_L, elem_R)
                divide_in_chunks(chunk_length, tmp_elem_L, tmp_elem_R)
                
 
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

