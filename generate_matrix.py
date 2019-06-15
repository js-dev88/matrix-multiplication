import argparse
import time
from pyspark import SparkContext
import sys
import random
import os
import shutil


def main(sc, side, name, nb_lines, nb_columns, keep):
    print(f'les arguments suivants ont été passés : nb_lines = {nb_lines}, nb_col={nb_columns}')
    # Évaluation du temps de création des matrices
    startTime = time.time()
    generate_matrix(sc, side, name, nb_lines, nb_columns)
    endTime = time.time()
    print('Temps total d\'exécution des scripts de création : ', str(int(endTime - startTime)) + 's')
    # sert uniquement pour garder spark web UI en local
    if keep:
        keep_web_ui_alive()


def generate_matrix(sc, side, name, nb_lines, nb_columns):
    # génération des occurences sous la forme (clé, valeur) => ((i, j), v)
    # v est une valeur entière entre 0 et 100
    matrix = sc.parallelize([((i,j),random.randint(0,101))  for i in range(1, nb_lines+1) for j in range(1, nb_columns+1)])
    # les couples ayant des valeurs égales à 0 sont filtrés
    matrix = matrix.filter(lambda row: row[1] != 0)
    # Mise sous format i j v des couples
    matrix = matrix.map(lambda row: str(row[0][0]) + '\t' + str(row[0][1]) + '\t' + str(row[1]))
    # Écriture dans le fichier
    filePath = f'spark/input/{side}_matrix_{name}_{nb_lines}_{nb_columns}'
    message = 'Fichier non généré'
    write_and_replace_if_exist(filePath, matrix, message)

    

def write_and_replace_if_exist(filePath, matrix, message):
    try:
        #Vérification matrice déjà présente
        print(filePath)
        print(os.path.exists(filePath))
        if os.path.exists(filePath):
            shutil.rmtree(filePath, ignore_errors=True)
        #écriture dans le fichier
        matrix.saveAsTextFile(filePath)
        print(f'{filePath} généré')
    except Exception as e: 
        print(e)
        print(message) 
    
def keep_web_ui_alive():
    rep = input()


if __name__ == '__main__':
    # Définition et Récupération des arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--side', type=str, required=True, default="L", help="L for left matrix or R for right matrix")
    parser.add_argument('-n', '--name', type=str, required=True, default='no_name', help="name of the matrix")
    parser.add_argument('-l', '--nb_lines', type=int, required=True, default=10, help="number of lines of the matrix")
    parser.add_argument('-c', '--nb_col', type=int, required=True, default=10, help="number of columns of the matrix")
    parser.add_argument('-k', '--keep', help="keep the program running for Spark web UI", action="store_true")
    args = parser.parse_args()

    # Création du spark context - lancement de l'interface spark UI
    sc = SparkContext()

    #exécution du main
    main(sc, args.side, args.name, args.nb_lines, args.nb_col, args.keep)
    
    # Arrêt du spark context 
    sc.stop()







