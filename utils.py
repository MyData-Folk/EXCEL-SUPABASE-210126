import re
import unicodedata
import pandas as pd
import math
from datetime import datetime

def json_safe(obj):
    """
    Rend un objet (dict, list, etc.) compatible JSON en remplaçant 
    les NaN, Inf et NaT par None.
    """
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_safe(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif pd.isna(obj): # Gère NaT et NaN de pandas
        return None
    return obj

def snake_case(text):
    """
    Convertit un texte en snake_case.
    Ex: "Date d'achat" -> "date_d_achat"
    Gère aussi les objets date pour éviter le format _000000
    Limite à 63 caractères pour PostgreSQL.
    """
    if not text:
        return text
    
    # Si c'est déjà une date ou un timestamp, formatage propre
    if isinstance(text, (datetime, pd.Timestamp)):
        return text.strftime('%Y_%m_%d')
    
    text = str(text)
    
    # Détection heuristique de chaîne de date (ex: "2026-01-16 00:00:00")
    if ' ' in text and (':' in text or '-' in text):
        try:
            # Essayer de voir si c'est une date qui a été stringifiée par pandas
            d = pd.to_datetime(text)
            return d.strftime('%Y_%m_%d')
        except:
            pass

    # Supprimer les caractères spéciaux et accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remplacer les espaces et caractères spéciaux par des underscores
    text = re.sub(r'[\s\-]+', '_', text)
    text = re.sub(r'[^a-zA-Z0-9_]', '', text)
    
    # Lowercase et Troncature à 63 caractères (Limite Postgres)
    return text.lower()[:63].strip('_')
