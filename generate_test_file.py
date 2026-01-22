import pandas as pd
from datetime import datetime

# Création d'un DataFrame avec des colonnes problématiques
data = {
    "16/01/2026": [1, 2, 3],
    "tarif_flex_le_plus_bas_folkestone_opra_los_1_bookingcom_eur_primary_compset": [10.5, 20.0, 15.75],
    "Nom court": ["A", "B", "C"]
}

df = pd.DataFrame(data)

# Sauvegarde en Excel
df.to_excel("test_import.xlsx", index=False)
print("Fichier test_import.xlsx créé avec succès.")
