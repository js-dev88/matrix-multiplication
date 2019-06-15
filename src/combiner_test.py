#!/usr/bin/env python3

import sys

p = int(sys.argv[1]) #2 #Number of lines of matrix M
q = int(sys.argv[2]) #3 #Number of columns of matrix M and nb lines of matrix N
r = int(sys.argv[3]) #2 #Number of columns of matrix N

for line in sys.stdin:
	line = line.strip()
	row = line.split('\t')
	f = open("/root/bigdata/combiner.txt", "a")
	if row[0] == 'L':
		for col in range(1, r+1):
			f.write('{key1},{key2}\t{val1},{val2},{val3}\n'.format(key1 = row[1], key2 = col, val1 = row[0], val2 = row[2], val3= row[3]))
			print('{key1},{key2}\t{val1},{val2},{val3}'.format(key1 = row[1], key2 = col, val1 = row[0], val2 = row[2], val3= row[3]))
				
	elif row[0] == 'R':
		for lin in range(1, p+1):
			f.write('{key1},{key2}\t{val1},{val2},{val3}\n'.format(key1 = lin, key2 = row[2], val1 = row[0], val2 = row[1], val3= row[3]))
			print('{key1},{key2}\t{val1},{val2},{val3}'.format(key1 = lin, key2 = row[2], val1 = row[0], val2 = row[1], val3= row[3]))
			
	f.close()
