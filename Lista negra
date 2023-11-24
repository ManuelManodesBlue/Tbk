# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 13:13:55 2023

@author: manuel.manodes
"""
import pandas as pd

# Leemos los archivos de fuente de entrada, así mismo debemos validar que los encabezados sean correctos y limpiar de espacios.
ruta_lista_negra = "C:/Users/manuel.manodes/Documents/Desarrollos/Local/Transbank/Auto/Anexo/Lista negra.csv"
lista_negra = pd.read_csv(ruta_lista_negra, delimiter=';', encoding='latin-1')

# Agrupar por 'ID' y mantener solo la última fila de cada grupo
lista_negra_2 = lista_negra.duplicated(subset='Comercio', keep='last')

# Crear un nuevo DataFrame sin duplicados, conservando solo el último registro repetido en la columna 'A'
lista_negra_2_sin_duplicados = lista_negra[~lista_negra_2]

# Imprimir el resultado
