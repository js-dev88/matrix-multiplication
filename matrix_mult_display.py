from pyspark import SparkContext
import argparse
import time
import sys


#Changer le chemin avant l'éxécution
PATH = 'root/bigdata/spark-2.4.3-bin-hadoop2.7/bin/projet/'
#PATH = 'home/jsa/bigdata/spark-2.4.1-bin-hadoop2.7/bin/projet/'

def main(sc, matrix_a, matrix_b, result):
    print(f'les arguments suivants ont été passés : matrix_a = {matrix_a}, matrix_b={matrix_b}, result = {result}')
    A = extract_matrix(sc, matrix_a)
    B = extract_matrix(sc, matrix_b) 
    R = extract_matrix(sc, result) 
    display_matrix(A, matrix_a)
    display_matrix(B, matrix_b)
    display_matrix(R, result)

def extract_matrix(sc, matrix_name):
    #Vérification de l'existence d'un fichier avec le nom de la matrice
    #Retourne un RDD avec le contenu de la matrice
    try:
        file_path = 'file:///' + PATH + matrix_name
        matrix_RDD = sc.textFile(file_path)
    except:
        print('Fichier non trouvé')

    return matrix_RDD



def display_matrix(matrix, matrix_name):
	row = 1
	string = "\n"+ matrix_name + " :\n"
	matrix_lines = matrix.collect()
	for line in matrix_lines:
		line = line.split('\t')
		if line[0]!= row:
			row = line[0]
			string += '\n'
			string += '{}\t'.format(line[2])
			#print('\n')
			#print('{}\t'.format(row[2]))
		else:
			string += '{}\t'.format(line[2])
			#print('{}\t'.format(row[2]))
	print(string)
		
		
	
if __name__ == '__main__':
    # Définition et Récupération des arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--matrix_a', type=str, required=True, help="name of the first matrix")
    parser.add_argument('-b', '--matrix_b', type=str, required=True, help="name of the second matrix")
    parser.add_argument('-r', '--matrix_result', type=str, required=True, help="name of the first matrix")
    args = parser.parse_args()

    # Création du spark context - lancement de l'interface spark UI
    sc = SparkContext()

    #exécution du main
    main(sc, args.matrix_a, args.matrix_b, args.matrix_result)
    
    # Arrêt du spark context 
    sc.stop()
