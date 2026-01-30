import re
import unicodedata
import pandas as pd
import math
from datetime import datetime

def json_safe(obj):
    """
    Rend un objet (dict, list, etc.) compatible JSON en remplaçant 
    les NaN, Inf et NaT par None.
    CORRECTION: Gère explicitement NaT (Not a Time) avant d'essayer .isoformat()
    """
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_safe(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (datetime, pd.Timestamp)):
        # CORRECTION: Vérifier si c'est NaT (Not a Time) avant d'essayer .isoformat()
        if isinstance(obj, pd.Timestamp) and pd.isna(obj):
            return None
        try:
            return obj.isoformat()
        except (ValueError, AttributeError):
            return None
    # CORRECTION: Gérer les types pandas NaT natifs
    elif isinstance(obj, pd.NaTType):
        return None
    # CORRECTION: Gérer les types pandas natifs
    elif isinstance(obj, (pd.Series, pd.DataFrame)):
        # Pour les Series/DataFrames, on ne les sérialise pas directement à l'extérieur
        # On les convertit en dict/list récursivement
        return str(obj) if pd.isna(obj) else obj
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
