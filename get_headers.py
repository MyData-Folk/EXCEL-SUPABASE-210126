import pandas as pd
import os

files = [
    'MODELE DE FICHIER EXCEL/RAPPORT RESERVATIONS EN COURS D-EDGE.xlsx',
    'MODELE DE FICHIER EXCEL/DATE SALONS ET EVENEMENTS.xlsx'
]

for f in files:
    if os.path.exists(f):
        try:
            df = pd.read_excel(f, nrows=0)
            print(f"File: {f}")
            print(f"Headers: {list(df.columns)}")
        except Exception as e:
            print(f"Error reading {f}: {e}")
    else:
        print(f"File not found: {f}")
