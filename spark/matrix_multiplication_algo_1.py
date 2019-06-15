from pyspark import SparkContext
import argparse
import time
import sys
from generate_matrix import keep_web_ui_alive, write_and_replace_if_exist
import matrix_mult_display


#Changer le chemin avant l'éxécution
PATH = 'root/bigdata/spark-2.4.3-bin-hadoop2.7/bin/projet/'
#PATH = 'home/jsa/bigdata/spark-2.4.1-bin-hadoop2.7/bin/projet/'

def main(sc, matrix_a, matrix_b, keep):
    print(f'les arguments suivants ont été passés : matrix_a = {matrix_a}, matrix_b={matrix_b}')
    #vérifications des dimensions de la matrice
    A, B = check_validity(matrix_a, matrix_b)
    #Multiplication des matrices
    startTime = time.time()
    mul = multiply_matrix(sc, A, B)
    endTime = time.time()
    print('Temps total d\'exécution du calcul du produit des matrices A et B : ', str(int(endTime - startTime)) + 's')
    #écriture dans un fichier de résultats
    write_in_file(sc, mul, A, B)
    # sert uniquement pour garder spark web UI en local
    result_file_name = 'result_' + A['name'] + '_X_' + B['name']
    matrix_mult_display.main(sc, matrix_a, matrix_b, result_file_name)
    if keep:
        keep_web_ui_alive()



def check_validity(matrix_a, matrix_b):
    #Vérification de la validité des dimensions
    #Renvoie deux dicts contenant les deux matrices et leurs caractéristiques
    name_elements_a = matrix_a.split('_')
    name_elements_b = matrix_b.split('_')

    #Le nombre de colonnes de A doit être égale au nombre de lignes de B
    if name_elements_a[3] != name_elements_b[2]:
        print("Multiplication impossible en raison des dimensions")
        sys.exit()
    else:
        A = craft_matrix(matrix_a, name_elements_a)
        B = craft_matrix(matrix_b, name_elements_b)

    return A, B


def craft_matrix(file_name, name_elements):
    #récupère les caractéritistiques d'une matrice et renvoie un dict avec les différents éléments
    matrice = {'file_name' : file_name, 'name' : name_elements[1], 'nb_lines' : name_elements[2], 'nb_columns' : name_elements[3]}
    
    return matrice


def multiply_matrix(sc, matrix_a, matrix_b):
    #Vérification et extraction des RDD
    A = extract_matrix(sc, matrix_a)
    B = extract_matrix(sc, matrix_b) 

    #split du RDD A selon les tabulations : (i, j , v)
    startTime = time.time()
    A = A.map(lambda x: x.split('\t'))
    #Mise sous la forme (j, ('A', i, v)) 
    A = A.map(lambda x:(x[1], ('A', x[0], x[2])))
    endTime = time.time()
    print('Temps total d\'exécution de la mise en forme de la matrice A : ', str(int(endTime - startTime)) + 's')
    
    #split du RDD B selon les tabulations : (i, j , v)
    startTime = time.time()
    B = B.map(lambda x: x.split('\t'))
    #Mise sous la forme (i, ('B', j, v)) 
    B = B.map(lambda x:(x[0], ('B', x[1], x[2])))
    endTime = time.time()
    print('Temps total d\'exécution de la mise en forme de la matrice B : ', str(int(endTime - startTime)) + 's')

    #Nous voulons joindre les éléments de la colonne j de A avec les élements de la ligne i de B avec i = j
    #Exemple : A11 (colonne 1) doit être multiplié avec tous les éléments de la ligne 1 de B (B11, B12, B13...)
    #Format : [('1', (('A', '1', '90'), ('B', '1', '74'))),..]
    startTime = time.time()
    mul = A.join(B)
    endTime = time.time()
    print('Temps total d\'exécution de la jointure des éléments de la matrice A avec ceux de la matrice B : ', str(int(endTime - startTime)) + 's')

    #Nous voulons maintenant regrouper les éléments par position dans la matrice de résultats
    #Par exemple l'élement RESij aura pour indice la ligne i de A et la colonne j de B
    #Les valeurs correspondantes sont multipliées 
    startTime = time.time()
    mul = mul.map(lambda x: ((x[1][0][1],x[1][1][1]), int(x[1][0][2]) * int(x[1][1][2])))
    endTime = time.time()
    print('Temps total d\'exécution du calcul des éléments Aij * Bjk : ', str(int(endTime - startTime)) + 's')
    
    #Nous avons donc toutes les valeurs pour chaque éléments de la matrice de résultats
    #Il reste à regrouper les clés, et à sommer les éléments
    startTime = time.time()
    mul = mul.reduceByKey(lambda x,y: x+y)
    endTime = time.time()
    print('Temps total d\'exécution de la somme des éléments Aij * Bjk (=Rik) : ', str(int(endTime - startTime)) + 's')
    
    #Tri des tuples clés par ordre croissant
    startTime = time.time()
    mul = mul.sortByKey()
    print('Temps total d\'exécution du tri des résultats selon leurs indices : ', str(int(endTime - startTime)) + 's')
    return mul


def extract_matrix(sc, matrix):
    #Vérification de l'existence d'un fichier avec le nom de la matrice
    #Retourne un RDD avec le contenu de la matrice
    try:
        file_path = 'file:///' + PATH + matrix['file_name']
        matrix_RDD = sc.textFile(file_path)
    except:
        print('Fichier non trouvé')

    return matrix_RDD

def write_in_file(sc, mul, A, B):
    # mise sous la forme normalisée de la matrice
    mul = mul.map(lambda row: str(row[0][0]) + '\t' + str(row[0][1]) + '\t' + str(row[1]))
    #Écriture dans un fichier résultat
    filePath = 'projet/result_' + A['name'] + '_X_' + B['name']
    message = 'Fichier résultats non généré'
    write_and_replace_if_exist(filePath, mul, message)
 
if __name__ == '__main__':
    # Définition et Récupération des arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--matrix_a', type=str, required=True, help="name of the first matrix")
    parser.add_argument('-b', '--matrix_b', type=str, required=True, help="name of the second matrix")
    parser.add_argument('-k', '--keep', help="keep the program running for Spark web UI", action="store_true")
    args = parser.parse_args()

    # Création du spark context - lancement de l'interface spark UI
    sc = SparkContext()

    #exécution du main
    main(sc, args.matrix_a, args.matrix_b, args.keep)
    
    # Arrêt du spark context 
    sc.stop()
