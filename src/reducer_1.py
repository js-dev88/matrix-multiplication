#!/usr/bin/env python3
import sys

current_line_matrix = 0
line_matrix = 0
current_col = 0
col = 0 
first_element = True
sum = 0

# lecture STDIN
for line in sys.stdin:
    element = line.split('\t')
    keys = element[0].split(',')
    line_matrix = keys[0]
    col = keys[1]
    value = int(element[1].rstrip())

    if first_element or ( current_line_matrix == line_matrix and current_col == col ):
        first_element = False
        sum += value
    else:
        print('{}\t{}\t{}'.format(current_line_matrix, current_col, sum))
        sum = value
    current_line_matrix = line_matrix
    current_col = col 

print('{}\t{}\t{}'.format(current_line_matrix, current_col, sum))