#!/usr/bin/env python3
import sys

# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split key , value
    element = line.split('\t')
    index = element[0].split(',')
    res_line = index[0]
    secondary_index = index[1]
    list_elem_by_line = eval(element[1])
    sum = 0
    for el in list_elem_by_line:
        sum += int(el)

    print('{}\t{}\t{}'.format(res_line, secondary_index, sum))
   


        
