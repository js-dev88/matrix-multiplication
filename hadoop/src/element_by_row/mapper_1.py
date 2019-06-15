#!/usr/bin/env python3
import sys

# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # split key , value
    elements = line.split('\t')
    liste_mul = []
   
    tuple_L = eval(elements[0])
    liste_R = eval(elements[1])
    current_line = tuple_L[1]
    for el_R in liste_R:
        liste_mul.append((el_R[1], int(tuple_L[2]) * int(el_R[2])))
             
    print('{}\t{}'.format(current_line,liste_mul))

    