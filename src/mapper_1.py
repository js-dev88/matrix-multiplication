#!/usr/bin/env python3
import sys

# lecture STDIN
for line in sys.stdin:
    elements = line.split('\t')
    mat_line = elements[0]
    values = elements[1].split(',')
    col = values[0]
    if len(values) > 0:
        multiplication = int(values[1]) * int(values[2].rstrip())
        print('{},{}\t{}'.format(mat_line, col, multiplication))

    