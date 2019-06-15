#!/usr/bin/env python3
import sys

# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    
    # split key , value
    elements = line.split('\t')
    tuple_L = eval(elements[0])
    tuple_R = eval(elements[1])
    
    print('{},{}\t{}'.format(int(tuple_L[1]), int(tuple_R[1]), int(tuple_L[2]) * int(tuple_R[2])))

    