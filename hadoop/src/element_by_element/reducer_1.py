#!/usr/bin/env python3
import sys

first_line = True
sum = 0
current_res_line = 0
current_res_col = 0

# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split key , value
    element = line.split('\t')
    keys = element[0].split(',')
    res_line = int(keys[0])
    res_col = int(keys[1])
    value = int(element[1])

    if current_res_line != res_line or current_res_col != res_col:
        if not first_line:
            print('{}\t{}\t{}'.format(current_res_line, current_res_col, sum))
        else:
            first_line = False
        sum = 0
        current_res_line = res_line
        current_res_col = res_col

    if current_res_line == res_line and current_res_col == res_col:
        sum += value

print('{}\t{}\t{}'.format(current_res_line, current_res_col, sum))