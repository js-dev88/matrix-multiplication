#!/usr/bin/env python3
import sys
first_line = True
current_index=0
current_secondary_index = 0
sum = 0

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

    if current_secondary_index != secondary_index or current_index != res_line:
        if not first_line:
            print('{}\t{}\t{}'.format(current_index, current_secondary_index, sum))
            sum = 0
        else:
            first_line = False
        current_index = res_line
        current_secondary_index = secondary_index     

    if current_secondary_index == secondary_index and current_index == res_line:
        for el in list_elem_by_line:
            sum += int(el)

print('{}\t{}\t{}'.format(current_index, current_secondary_index, sum))
    
    

 
   


        
