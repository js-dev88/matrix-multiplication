#!/usr/bin/env python3
import sys

first_line = True
sum_list = {}
current_res_line = 0


# lecture STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split key , value
    element = line.split('\t')
    res_line = int(element[0])
    list_coeff_by_line = eval(element[1])

    if current_res_line != res_line:
        if not first_line:
            for col_index, sum in sum_list.items():
                print('{}\t{}\t{}'.format(current_res_line, col_index, sum))
        else:
            first_line = False                
        sum_list = {}
        current_res_line = res_line

    if current_res_line == res_line:
        for el_res in list_coeff_by_line:
            try:
                sum_list[el_res[0]] += el_res[1]
            except KeyError:
                sum_list[el_res[0]] = el_res[1]
        

for col_index, sum in sum_list.items():
    print('{}\t{}\t{}'.format(current_res_line, col_index, sum))