import re
import unicodedata
import pandas as pd
import math
from datetime import datetime

def json_safe(obj):
    """
    Rend un objet compatible JSON en remplaçant 
    les NaN, Inf et NaT par None de manière robuste.
    """
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_safe(v) for v in obj]
    
    # Cas des types "non-valeurs" de pandas (NaN, NaT, None, NA)
    if pd.isna(obj):
        return None
        
    if isinstance(obj, float):
        if math.isinf(obj):
            return None
        return obj
    
    if isinstance(obj, (datetime, pd.Timestamp)):
        try:
            return obj.isoformat()
        except:
            return None
            
    if isinstance(obj, (pd.Series, pd.DataFrame)):
        return obj.to_dict()
        
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
    
    # CORRECTION: Gérer les objets datetime correctement avant de convertir en string
    if isinstance(text, (datetime, pd.Timestamp)):
        # Utiliser isoformat pour le format standard YYYY-MM-DD
        if isinstance(text, pd.Timestamp) and pd.isna(text):
            return 'date_n_a'
        try:
            return text.isoformat().split('T')[0].replace('-', '_')
        except:
            return 'date_n_a'
    
    text = str(text)
    
    # Détection heuristique de chaîne de date (ex: "2026-01-16 00:00:00")
    if ' ' in text and (':' in text or '-' in text):
        try:
            # Essayer de voir si c'est une date qui a été stringifiée par pandas
            d = pd.to_datetime(text)
            # Format YYYY_MM_DD
            return d.strftime('%Y_%m_%d')
        except:
            pass

    # Supprimer les caractères spéciaux et accents
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remplacer les espaces et caractères spéciaux par des underscores
    text = re.sub(r'[\s\-]+', '_', text)
    text = re.sub(r'[^a-zA-Z0-9_]', '', text)
    
    # Lowercase et Troncature à 63 caractères (Limite Postgres)
    return text.lower()[:63].strip('_')
