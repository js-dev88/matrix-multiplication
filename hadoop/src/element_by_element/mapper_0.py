#!/usr/bin/env python3
import sys
import os

#Récupération du type de la matrice
filepath = os.environ["map_input_file"] 
filename = os.path.split(filepath)[-1]
type_matrix = filename.split('_')[0]

# lecture STDIN
for line in sys.stdin:
    # split the line into matrix element
    element = line.split('\t')
    # we want pair as (j, (i, v)) for the first matrix, (i, (j, v)) for the second matrix
    if type_matrix == 'L':
        print('{}\t{},{},{}'.format(element[1], element[0], element[2].rstrip(), 'L'))
    if type_matrix == 'R':
        print('{}\t{},{},{}'.format(element[0], element[1], element[2].rstrip(), 'R'))

    
"""hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar \
-input input/L_matrix_a_3_3.txt -input input/R_matrix_b_3_3.txt \
-output output/ \
-file src/mapper_0.py -mapper src/mapper_0.py \
-file src/reducer_0.py -reducer src/reducer_0.py 

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar \
-input output/part* \
-output output2/ \
-file src/mapper_1.py -mapper src/mapper_1.py \
-file src/reducer_1.py -reducer src/reducer_1.py """