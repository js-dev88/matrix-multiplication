# -*- coding: utf-8 -*-
from pyspark import SparkContext
import argparse
import time
import sys
from generate_matrix import keep_web_ui_alive, write_and_replace_if_exist
import matrix_mult_display


#Changer le chemin avant l'éxécution
PATH = '/root/bigdata/'
#PATH = '/home/cluster/user88/'
#PATH = 'home/jsa/bigdata/spark-2.4.1-bin-hadoop2.7/bin/projet/'

INFORMER_log = None
TIMER_log = None
DEBUG_log = None
ERROR_log = None

def main(sc, matrix_a, matrix_b, keep, algo, n_chunk=None):
    INFORMER_log.info("\n========================================\n")
    DEBUG_log.info("\n========================================\n")
    ERROR_log.info("\n========================================\n")
    INFORMER_log.info('les arguments suivants ont été passés : matrix_a = {}, matrix_b={}'.format(matrix_a, matrix_b))
    INFORMER_log.info('Algorithme utilisé : {} ; Nombre de chunk :{}'.format(algo, n_chunk))
    #vérifications des dimensions de la matrice
    A, B = check_validity(matrix_a, matrix_b)
    #Multiplication des matrices
    startTime = time.time()
    mul = multiply_matrix(sc, A, B, algo, n_chunk)
    endTime = time.time()
    TIMER_log.info('Temps total d\'exécution du calcul du produit des matrices {} et {} : {} s'.format(A['name'], B['name'], str(float(endTime - startTime))))
    #écriture dans un fichier de résultats
    write_in_file(sc, mul, A, B)
    result_file_name = 'projet/result_' + A['name'] + '_X_' + B['name'] + '_' + A['nb_lines'] + '_' + B['nb_columns']
    #result_file_name = 'result_' + A['name'] + '_X_' + B['name'] + '_' + A['nb_lines'] + '_' + B['nb_columns']
    #matrix_mult_display.main(sc, matrix_a, matrix_b, result_file_name)
    # sert uniquement pour garder spark web UI en local
    if keep:
        keep_web_ui_alive()



def check_validity(matrix_a, matrix_b):
    #Vérification de la validité des dimensions
    #Renvoie deux dicts contenant les deux matrices et leurs caractéristiques
    name_elements_a = matrix_a.split('_')
    name_elements_b = matrix_b.split('_')

    #Le nombre de colonnes de A doit être égale au nombre de lignes de B
    if name_elements_a[4] != name_elements_b[3]:
        ERROR_log.error("Multiplication impossible en raison des dimensions")
        print("Multiplication impossible en raison des dimensions")
        sys.exit()
    else:
        A = craft_matrix(matrix_a, name_elements_a)
        B = craft_matrix(matrix_b, name_elements_b)

    return A, B


def craft_matrix(file_name, name_elements):
    #récupère les caractéritistiques d'une matrice et renvoie un dict avec les différents éléments
    #matrice = {'file_name' : file_name, 'type': name_elements[0], 'name' : name_elements[2], 'nb_lines' : name_elements[3], 'nb_columns' : name_elements[4]}
    matrice = {'file_name' : file_name, 'name' : name_elements[2], 'nb_lines' : name_elements[3], 'nb_columns' : name_elements[4]}
    DEBUG_log.debug("TEST DU DEBUG")
    DEBUG_log.debug(matrice)
    
    return matrice


def multiply_matrix(sc, matrix_a, matrix_b, algo, n_chunk=None):
    #Vérification et extraction des RDD
    startTime = time.time()
    A = extract_matrix(sc, matrix_a)
    B = extract_matrix(sc, matrix_b) 
    endTime = time.time()
    TIMER_log.info('Temps total d\'exécution chargement matrices : {} s'.format(str(float(endTime - startTime))))
    if int(algo) == 0:
        startTime = time.time()
        mul = multiply_element_by_element(A, B)
        endTime = time.time()
        TIMER_log.info('Temps total d\'exécution calcul matrice ALGO ELEMENT BY ELEMENT : {} s'.format(str(float(endTime - startTime))))
    elif int(algo) == 1:
        startTime = time.time()
        mul = multiply_row_by_column(A, B)
        endTime = time.time()
        TIMER_log.info('Temps total d\'exécution calcul matrice ALGO ROW BY COLUMN : {} s'.format(str(float(endTime - startTime))))
    elif int(algo) == 2:
        startTime = time.time()
        mul = multiply_element_by_row(A, B)
        endTime = time.time()
        TIMER_log.info('Temps total d\'exécution calcul matrice ALGO ELEMENT BY ROW : {} s'.format(str(float(endTime - startTime))))
    elif int(algo) == 3:
        startTime = time.time()
        mul = multiply_row_block_by_col_block(A, B, n_chunk, int(matrix_a['nb_lines']), int(matrix_a['nb_columns']),  int(matrix_b['nb_lines']), int(matrix_b['nb_columns']))
        endTime = time.time()
        TIMER_log.info('Temps total d\'exécution calcul matrice ALGO ROW BLOCK BY COLUMN BLOCK : {} s'.format(str(float(endTime - startTime))))
    else:
        print('Algorithm introuvable')

    return mul

def multiply_row_block_by_col_block(A, B, n_chunk, nb_lineA, nb_colA, nb_lineB, nb_colB):

    if n_chunk is None:
        n_chunk = 2

    A = A.map(lambda x: x.split('\t')) 
    A = A.map(lambda x:((int(x[0]),int(x[1])), int(x[2])))
    A_t = sc.parallelize(((x,y),0) for x in range(1,nb_lineA) for y in range(1,nb_colA))
    A_zero = A.union(A_t).reduceByKey(lambda x,y: x+y).sortByKey().map(lambda x:(x[0][0], (x[0][1], x[1])))
    A_line = A_zero.groupByKey().flatMap(lambda x : [((x[0], i), y) for i, y in enumerate([list(x[1])[i:i+int(n_chunk)] for i in range(0, len(list(x[1])), int(n_chunk))])])
    A_line = A_line.flatMap(lambda x: [(x[0],y) for y in x[1] if y[1] != 0]).groupByKey().map(lambda x : (x[0], list(x[1])))

    #B = sc.textFile('projet/R_matrix_TEST1b_3_4')
    B = B.map(lambda x: x.split('\t'))  
    B = B.map(lambda x:((int(x[1]), int(x[0])), int(x[2])))
    B_t = sc.parallelize(((x,y),0) for x in range(1,nb_colB) for y in range(1,nb_lineB))
    B_zero = B.union(B_t).reduceByKey(lambda x,y: x+y).sortByKey().map(lambda x:(x[0][0], (x[0][1], x[1])))
    B_col = B_zero.groupByKey().flatMap(lambda x : [((x[0], i), y) for i, y in enumerate([list(x[1])[i:i+int(n_chunk)] for i in range(0, len(list(x[1])), int(n_chunk))])])
    B_col = B_col.flatMap(lambda x: [(x[0],y) for y in x[1] if y[1] != 0]).groupByKey().map(lambda x : (x[0], list(x[1])))
    
    mul = A_line.cartesian(B_col)
    mul = mul.filter(lambda x: x[0][0][1] == x[1][0][1])
    mul = mul.map(lambda x: ((x[0][0][0], x[1][0][0]), [int(y[1]) * int(z[1]) for y in x[0][1] for z in x[1][1] if y[0] == z[0] ]))
    mul = mul.reduceByKey(lambda x,y: x+y)
    mul = mul.map(lambda x: (x[0], sum(x[1])))
    mul = mul.filter(lambda x : x[1] !=0).sortByKey()
    mul.take(10)

    return mul

def multiply_row_by_column(A, B):
    #Mise sous la forme (i, ( j, v)) 
    A = A.map(lambda x: x.split('\t'))  
    A = A.map(lambda x:(x[0], (x[1], x[2])))
    A_line = A.groupByKey().map(lambda x : (x[0], list(x[1])))

    #Mise sous la forme (j, ( i, v)) 
    B = B.map(lambda x: x.split('\t'))  
    B = B.map(lambda x:(x[1], (x[0], x[2])))
    B_col = B.groupByKey().map(lambda x : (x[0], list(x[1])))

    #produit cartésien entre les vecteurs lignes et les vecteurs colonnes
    mul = A_line.cartesian(B_col)

    #Chaque ligne du rdd correspond à un élément de la matrice finale
    #Chaque élément de la ligne est multiplié pair à pair avec l'élément de la colonne correspondante
    mul = mul.map(lambda x: ((x[0][0], x[1][0]), [int(y[1]) * int(z[1]) for y in x[0][1] for z in x[1][1] if y[0] == z[0]]))
    #Sum des produits de la liste
    mul = mul.map(lambda x: (x[0], sum(x[1])))
    #Tri des valeurs
    mul = mul.sortByKey()

    return mul

def multiply_element_by_row(A, B):
    #split du RDD A selon les tabulations : (i, j , v)
    A = A.map(lambda x: x.split('\t'))
    #Mise sous la forme (j, ( i, v)) 
    A = A.map(lambda x:(x[1], (x[0], x[2])))
    
    #split du RDD B selon les tabulations : (i, j , v)
    B = B.map(lambda x: x.split('\t'))
    #Mise sous la forme (i, (j, v)) 
    B = B.map(lambda x:(x[0], (x[1], x[2])))

    #Nous voulons joindre les éléments de la colonne j de A avec les élements de la ligne i de B avec i = j
    #Exemple : A11 (colonne 1) doit être multiplié avec tous les éléments de la ligne 1 de B (B11, B12, B13...)
    #Format : [('1', (1', '90'), (1', '74'))),..]
    startTime = time.time()
    mul = A.join(B)
    endTime = time.time()
    print('Temps total d\'exécution de la jointure des éléments de la matrice A avec ceux de la matrice B : ', str(int(endTime - startTime)) + 's')
    
    #map pour avoir la forme ((index_column_A, index_ligne_A, valeur_A)(index_colonne_B, valeur)
    mul = mul.map(lambda x : ((x[0], x[1][0][0], x[1][0][1]), (x[1][1][0], x[1][1][1])))

    #regrouper les enregistrements par clé
    mul = mul.groupByKey().map(lambda x : (x[0], list(x[1])))

    #on distribue la multiplication de lélement sur tous les élements de la lignes pour obtenir la forme 
    #((resi, resj), value)
    mul = mul.map(lambda x : [((x[0][1], y[0]), int(x[0][2]) * int(y[1])) for y in x[1]])

    #on aplatit la liste pour avoir des couples clé , valeur
    mul = mul.flatMap(lambda x : [y for y in x])

    #enfin on somme sur tous les éléments ayant la même clé
    mul = mul.reduceByKey(lambda x,y: x+y)

    #Tri des valeurs
    mul = mul.sortByKey()
    return mul

def multiply_element_by_element(A, B):

    #split du RDD A selon les tabulations : (i, j , v)
    startTime = time.time()
    A = A.map(lambda x: x.split('\t'))
    #Mise sous la forme (j, ( i, v)) 
    A = A.map(lambda x:(x[1], (x[0], x[2])))
    endTime = time.time()
    #print('Temps total d\'exécution de la mise en forme de la matrice A : ', str(int(endTime - startTime)) + 's')
    
    #split du RDD B selon les tabulations : (i, j , v)
    startTime = time.time()
    B = B.map(lambda x: x.split('\t'))
    #Mise sous la forme (i, (j, v)) 
    B = B.map(lambda x:(x[0], (x[1], x[2])))
    endTime = time.time()
    #print('Temps total d\'exécution de la mise en forme de la matrice B : ', str(int(endTime - startTime)) + 's')

    #Nous voulons joindre les éléments de la colonne j de A avec les élements de la ligne i de B avec i = j
    #Exemple : A11 (colonne 1) doit être multiplié avec tous les éléments de la ligne 1 de B (B11, B12, B13...)
    #Format : [('1', (('A', '1', '90'), ('B', '1', '74'))),..]
    startTime = time.time()
    mul = A.join(B)
    endTime = time.time()
    #print('Temps total d\'exécution de la jointure des éléments de la matrice A avec ceux de la matrice B : ', str(int(endTime - startTime)) + 's')

    #Nous voulons maintenant regrouper les éléments par position dans la matrice de résultats
    #Par exemple l'élement RESij aura pour indice la ligne i de A et la colonne j de B
    #Les valeurs correspondantes sont multipliées 
    startTime = time.time()
    mul = mul.map(lambda x: ((x[1][0][0],x[1][1][0]), int(x[1][0][1]) * int(x[1][1][1])))
    endTime = time.time()
    #print('Temps total d\'exécution du calcul des éléments Aij * Bjk : ', str(int(endTime - startTime)) + 's')
    
    #Nous avons donc toutes les valeurs pour chaque éléments de la matrice de résultats
    #Il reste à regrouper les clés, et à sommer les éléments
    startTime = time.time()
    mul = mul.reduceByKey(lambda x,y: x+y)
    endTime = time.time()
    #print('Temps total d\'exécution de la somme des éléments Aij * Bjk (=Rik) : ', str(int(endTime - startTime)) + 's')
    
    #Tri des tuples clés par ordre croissant
    startTime = time.time()
    mul = mul.sortByKey()
    #print('Temps total d\'exécution du tri des résultats selon leurs indices : ', str(int(endTime - startTime)) + 's')
    return mul

def extract_matrix(sc, matrix):
    #Vérification de l'existence d'un fichier avec le nom de la matrice
    #Retourne un RDD avec le contenu de la matrice
    try:
        #file_path = 'file://' + PATH + matrix['file_name']
        file_path = PATH + matrix['file_name']
        print(file_path)
        matrix_RDD = sc.textFile(file_path)
    except:
        print('Fichier non trouvé')

    return matrix_RDD

def write_in_file(sc, mul, A, B):
    # mise sous la forme normalisée de la matrice
    mul = mul.map(lambda row: str(row[0][0]) + '\t' + str(row[0][1]) + '\t' + str(row[1]))
    #Écriture dans un fichier résultat
    #filePath = 'projet/result_' + A['name'] + '_X_' + B['name']
    #filePath = 'file://' + PATH + 'projet/result_' + A['name'] + '_X_' + B['name'] + '_' + A['nb_lines'] + '_' + B['nb_columns']
    filePath = PATH + 'projet/result_' + A['name'] + '_X_' + B['name'] + '_' + A['nb_lines'] + '_' + B['nb_columns']
    message = 'Fichier résultats non généré'
    write_and_replace_if_exist(filePath, mul, message)
 
if __name__ == '__main__':
    # Définition et Récupération des arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--matrix_a', type=str, required=True, help="name of the first matrix")
    parser.add_argument('-b', '--matrix_b', type=str, required=True, help="name of the second matrix")
    parser.add_argument('-k', '--keep', help="keep the program running for Spark web UI", action="store_true")
    parser.add_argument('-al', '--algo', required=True, type=int, help="Algorithm choice")
    parser.add_argument('-n', '--n_chunk', type=int, help="Length of block algorithm")
    args = parser.parse_args()

    # Création du spark context - lancement de l'interface spark UI
    sc = SparkContext()

    log4jLogger = sc._jvm.org.apache.log4j

    #Info general à remonter, ex : nom des fichiers, type d'algo utilisé, etc...
    INFORMER_log = log4jLogger.LogManager.getLogger("INFORMER")
    #Celui-ci pour envoyer les temps, envoie sur info_log comme INFORMER_log (mais il est identifié par "TIMER")
    TIMER_log = log4jLogger.LogManager.getLogger("TIMER")
    DEBUG_log = log4jLogger.LogManager.getLogger("DEBUGGER")
    ERROR_log = log4jLogger.LogManager.getLogger("ERROR_LOGGER")

    #exécution du main
    main(sc, args.matrix_a, args.matrix_b, args.keep, args.algo, args.n_chunk)
    
    # Arrêt du spark context 
    sc.stop()
