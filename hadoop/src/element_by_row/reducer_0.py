#!/usr/bin/env python3
import sys

nb_blocks = 2

def join(elements_L, elements_R):
    if len(elements_L) > 0 and len(elements_R) > 0:
            for el_l in elements_L:
                    print('{}\t{}'.format(el_l, elements_R))
 
#on fabrique les lignes et colonnes
current_index = 0
index = 0
elements_L = []
elements_R = []
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
            join(elements_L, elements_R)
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

join(elements_L, elements_R)

