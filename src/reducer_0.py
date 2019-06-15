#!/usr/bin/env python3
import sys

def join(index, list_L, list_R):
    """
    Effectue la jointure pour avoir une ligne de sortie de la forme
    (index, (colonne R, valeur L, Valeur R)) et imprime le résultat
    on choisit de paralléliser la multiplication dans un autre mapper
    """
    if len(list_L) > 0 and len(list_R) > 0:
        for element_L in list_L:
            for element_R in list_R:
                print('{}\t{},{},{}'.format(index, element_R[0], element_L, element_R[1]))

current_index = 0
index = 0
list_L = []
list_R = []
first_element = True

# lecture STDIN
for line in sys.stdin:
    element = line.split('\t')
    index = element[0]
    values = element[1].split(',')

    #On affecte l'indice et la valeur restante
    if  current_index != index and not first_element:
        #jointure et print des éléments joins
        join(current_index, list_L, list_R)
        #vider les listes
        list_L = []
        list_R = []
        #mise à jour de l'index
        current_index = index

    if  first_element or current_index == index:
        #gestion du cas particulier de la première ligne
        first_element = False
        #test si c'est un élément de L ou de R
        if values[2].rstrip() == 'L':
            #(i,v)
            list_L.append(values[1])
        else:
            #(j,v)
            list_R.append((values[0], values[1]))
        current_index = index

join(current_index, list_L, list_R)




