"""
Supabase Auto-Importer (RMS Sync) v2.0
Backend Flask - Application principale

Auteur: MiniMax Agent
Date: 21 Janvier 2026
"""

import os
import json
import csv
import io
import uuid
import re
import logging
from datetime import datetime
from pathlib import Path
from functools import wraps

import pandas as pd
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from supabase import create_client, Client

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chargement des variables d'environnement
load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH', 52428800))
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', './uploads')

# Configuration CORS
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Extensions autorisées
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
MAX_PREVIEW_ROWS = 10

# ============================================================================
# ROUTES STATIQUES
# ============================================================================

@app.route('/')
def index():
    """Sert la page principale de l'application."""
    return send_file('index.html')

@app.route('/favicon.ico')
def favicon():
    """Sert le favicon (retourne une réponse vide)."""
    return '', 204

# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def allowed_file(filename):
    """Vérifie si l'extension du fichier est autorisée."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def read_csv_robust(file_path, **kwargs):
    """
    Tente de lire un CSV de manière robuste (encodage, séparateur, bad lines).
    Utilise csv.Sniffer pour détecter le délimiteur.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    delimiters = [None, ',', ';', '\t', '|'] # None = auto-detect via Sniffer

    # Paramètres de base pour pandas
    read_params = kwargs.copy()
    read_params['on_bad_lines'] = 'skip'
    read_params['engine'] = 'python' # Moteur plus flexible que 'c'

    last_error = None

    for encoding in encodings:
        read_params['encoding'] = encoding
        
        # Tenter de détecter le délimiteur
        detected_sep = None
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                sample = f.read(2048)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    detected_sep = dialect.delimiter
                except csv.Error:
                    pass # Sniffer a échoué, on essaiera les délimiteurs par défaut
        except UnicodeDecodeError:
            continue # Encodage incorrect, suivant

        # Liste des séparateurs à tester pour cet encodage
        seps_to_try = [detected_sep] if detected_sep else delimiters[1:] # Si détecté, on priorise, sinon on teste tout
        if detected_sep and detected_sep not in seps_to_try:
             seps_to_try.append(detected_sep)

        for sep in seps_to_try:
            if sep:
                read_params['sep'] = sep
            
            try:
                # Test de lecture
                df = pd.read_csv(file_path, **read_params)
                if not df.empty and len(df.columns) > 1:
                     logger.info(f"CSV lu avec succès: enc={encoding}, sep='{sep}'")
                     return df
                elif not df.empty:
                     # Si une seule colonne, c'est suspect (sauf si le fichier n'a qu'une colonne)
                     # On sauvegarde ce résultat au cas où, mais on continue de chercher mieux
                     last_result = df
            except Exception as e:
                last_error = e
                continue
    
    # Si on arrive ici, soit on a un résultat 'moyen' (1 colonne), soit rien
    if 'last_result' in locals():
        return last_result
    
    # Echec total, on relève la dernière erreur ou une générique
    raise last_error or ValueError("Impossible de lire le fichier CSV (encodage/séparateur incompatible)")


def get_supabase_client():
    """Crée et retourne un client Supabase."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        error_msg = "Configuration Supabase manquante dans .env (SUPABASE_URL ou SUPABASE_KEY)"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        return create_client(supabase_url, supabase_key)
    except Exception as e:
        logger.error(f"Erreur d'initialisation du client Supabase: {str(e)}")
        raise


def ensure_upload_folder():
    """Crée le dossier d'upload s'il n'existe pas."""
    folder = app.config['UPLOAD_FOLDER']
    try:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
            logger.info(f"Dossier d'upload créé : {os.path.abspath(folder)}")
        else:
            # Vérifier les permissions en écriture
            if not os.access(folder, os.W_OK):
                logger.error(f"ERREUR : Le dossier {folder} n'est pas accessible en écriture")
            else:
                logger.debug(f"Dossier d'upload OK : {folder}")
    except Exception as e:
        logger.error(f"Erreur lors de la création du dossier {folder} : {str(e)}")


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
    import unicodedata
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remplacer les espaces et caractères spéciaux par des underscores
    text = re.sub(r'[\s\-]+', '_', text)
    text = re.sub(r'[^a-zA-Z0-9_]', '', text)
    
    # Lowercase et Troncature à 63 caractères (Limite Postgres)
    return text.lower()[:63].strip('_')


def clean_number(value):
    """
    Nettoie une valeur numérique.
    Gère les formats français (1 000,50) et anglais (1000.50).
    Supprime les devises et caractères non numériques.
    """
    if pd.isna(value):
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    value_str = str(value).strip()
    
    # Ne rien faire si vide
    if not value_str:
        return None
    
    # Supprimer les espaces insécables et milliers
    value_str = value_str.replace('\xa0', '').replace(' ', '')
    
    # Supprimer les symboles de devises
    value_str = re.sub(r'[€$£¥]', '', value_str)
    
    # Gérer le format français: virgule comme séparateur décimal
    if ',' in value_str and '.' in value_str:
        # Format: 1 000,50 ou 1,000.50
        if value_str.index(',') < value_str.index('.'):
            # Format français: 1 000,50 -> 1000.50
            value_str = value_str.replace(',', '.')
        else:
            # Format anglais: déjà correct
            pass
    elif ',' in value_str:
        # Probablement format français
        value_str = value_str.replace(',', '.')
    
    try:
        return float(value_str)
    except ValueError:
        return None


def parse_date(value):
    """
    Convertit différents formats de date vers le format SQL (YYYY-MM-DD).
    Gère: Excel serial dates, ISO dates, FR dates (JJ/MM/AAAA)
    """
    if pd.isna(value):
        return None
    
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    
    if isinstance(value, (int, float)):
        # Excel serial date (nombre de jours depuis 1899-12-30)
        try:
            excel_epoch = datetime(1899, 12, 30)
            date = excel_epoch + pd.Timedelta(days=int(value))
            return date.strftime('%Y-%m-%d')
        except:
            return None
    
    value_str = str(value).strip()
    
    if not value_str:
        return None
    
    # Liste des formats à essayer
    date_formats = [
        '%Y-%m-%d',      # ISO: 2026-01-21
        '%d/%m/%Y',      # FR: 21/01/2026
        '%d/%m/%y',      # FR court: 21/01/26
        '%m/%d/%Y',      # US: 01/21/2026
        '%Y/%m/%d',      # ISO alternatif
        '%d-%m-%Y',      # FR avec tirets
        '%d.%m.%Y',      # Format allemand
    ]
    
    for fmt in date_formats:
        try:
            date = datetime.strptime(value_str, fmt)
            return date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def parse_datetime(value):
    """
    Sépare une valeur datetime en date et heure.
    Retourne un tuple (date, heure) au format SQL.
    """
    if pd.isna(value):
        return None, None
    
    # Si c'est un timestamp Excel (nombre)
    if isinstance(value, (int, float)):
        try:
            excel_epoch = datetime(1899, 12, 30)
            dt = excel_epoch + pd.Timedelta(days=int(value))
            return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')
        except:
            return None, None
    
    # Si c'est une chaîne
    value_str = str(value).strip()
    
    # Essayer de parser directement comme datetime
    datetime_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d-%m-%Y %H:%M:%S',
    ]
    
    for fmt in datetime_formats:
        try:
            dt = datetime.strptime(value_str, fmt)
            return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')
        except ValueError:
            continue
    
    # Essayer comme date seulement
    date_only = parse_date(value_str)
    if date_only:
        return date_only, None
    
    return None, None


def clean_text(value):
    """
    Nettoie une valeur texte.
    Supprime les accents et caractères spéciaux.
    """
    if pd.isna(value):
        return None
    
    import unicodedata
    value_str = str(value).strip()
    
    # Normaliser les accents
    value_str = unicodedata.normalize('NFD', value_str)
    value_str = ''.join(c for c in value_str if unicodedata.category(c) != 'Mn')
    
    # Supprimer les caractères de contrôle
    value_str = ''.join(c for c in value_str if ord(c) >= 32)
    
    return value_str if value_str else None


def normalize_dataframe(df, column_types=None, column_mapping=None, split_datetime=False):
    """
    Normalise un DataFrame selon les règles de typage, mapping et split.
    """
    df = df.copy()
    
    # 1. Appliquer les types forcés sur les noms ORIGINAUX
    if column_types:
        for col, col_type in column_types.items():
            if col not in df.columns:
                continue
            
            if col_type == 'date':
                df[col] = df[col].apply(parse_date)
            elif col_type == 'numeric':
                df[col] = df[col].apply(clean_number)
            elif col_type == 'text':
                df[col] = df[col].apply(clean_text)
    
    # 2. Appliquer le mapping des colonnes ou snake_case par défaut
    if column_mapping:
        # On ne garde que les colonnes mappées qui existent dans le DF
        valid_mapping = {k: v for k, v in column_mapping.items() if v and k in df.columns}
        if valid_mapping:
            df = df[list(valid_mapping.keys())].rename(columns=valid_mapping)
        else:
            # Si aucune colonne ne correspond au mapping (ex: mismatch de clés), on logge et on force snake_case
            logger.warning(f"normalize_dataframe: valid_mapping est vide. Keys Frontend: {list(column_mapping.keys())}, Keys DF: {list(df.columns)}")
            df.columns = [snake_case(col) for col in df.columns]
    else:
        # Snake_case par défaut pour toutes les colonnes
        df.columns = [snake_case(col) for col in df.columns]

    # 3. Si split_datetime, détecter et séparer les colonnes temporelles
    if split_datetime:
        cols_to_process = list(df.columns)
        for col in cols_to_process:
            # Vérifier si la colonne contient des timestamps
            sample_values = df[col].dropna().head(10)
            if sample_values.empty:
                continue
            
            # Tester si c'est une colonne datetime
            has_date = False
            has_time = False
            
            for val in sample_values:
                if isinstance(val, (datetime, pd.Timestamp)):
                    has_date = True
                    if val.hour != 0 or val.minute != 0 or val.second != 0:
                        has_time = True
                    break
                
                if isinstance(val, (int, float)):
                    # Excel dates are numbers
                    try:
                        if 10000 < val < 60000: # Range for modern dates in Excel
                            has_date = True
                            if val % 1 != 0: has_time = True
                            break
                    except TypeError:
                        continue
                    continue
                
                val_str = str(val)
                # Formats courants: 2024-01-01 12:00:00 or 01/01/2024 12:00
                if (' ' in val_str or 'T' in val_str) and (':' in val_str):
                    has_date = True
                    has_time = True
                    break
                elif '/' in val_str or '-' in val_str:
                    if len(val_str) > 6: has_date = True
            
            if has_date and has_time:
                date_col = f"date_{col}"
                time_col = f"heure_{col}"
                
                # Séparer les valeurs
                dates = []
                heures = []
                for val in df[col]:
                    d, h = parse_datetime(val)
                    dates.append(d)
                    heures.append(h)
                
                df[date_col] = dates
                df[time_col] = heures
                
                # Supprimer la colonne originale
                df = df.drop(columns=[col])
    
    # Remplacer les valeurs NaN/None par None
    df = df.where(pd.notnull(df), None)
    
    return df


def dataframe_to_json_records(df):
    """
    Convertit un DataFrame en liste de dictionnaires pour Supabase.
    Nettoie agressivement les valeurs pour éviter les erreurs de génération JSON.
    """
    # 1. Remplacer les NaN globaux
    df = df.where(pd.notnull(df), None)
    
    records = df.to_dict(orient='records')
    clean_records = []
    
    for record in records:
        try:
            clean_record = {}
            for key, value in record.items():
                # Forcer la clé en string
                k = str(key)
                
                # Gérer les valeurs
                if value is None or (isinstance(value, float) and (pd.isna(value) or value != value)):
                    clean_record[k] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    # L'utilisateur ne veut pas l'heure, juste YYYY-MM-DD
                    clean_record[k] = value.strftime('%Y-%m-%d')
                elif isinstance(value, pd.Timedelta):
                    clean_record[k] = str(value)
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    # Nettoyage Inf et NaN pour les nombres
                    if value != value or value == float('inf') or value == float('-inf'):
                        clean_record[k] = None
                    else:
                        clean_record[k] = value
                elif isinstance(value, (str, bool)):
                    clean_record[k] = value
                else:
                    # Tout le reste en string
                    clean_record[k] = str(value)
            
            # Vérification finale de sérialisation JSON pour cet enregistrement
            try:
                # json.dumps a été déplacé en haut
                json.dumps(clean_record)
                clean_records.append(clean_record)
            except (TypeError, ValueError) as je:
                logger.warning(f"Serialization fallback pour un record: {str(je)}")
                # Si un enregistrement échoue encore, on force tout en string
                safe_record = {str(k): str(v) if v is not None else None for k, v in clean_record.items()}
                clean_records.append(safe_record)
                
        except Exception as e:
            logger.warning(f"Erreur lors du nettoyage d'une ligne: {str(e)}")
            continue
    
    return clean_records


# ============================================================================
# ROUTES API - FICHIERS
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Vérification de l'état du serveur et de la connexion Supabase.
    """
    status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0',
        'supabase': 'unknown'
    }
    
    try:
        supabase = get_supabase_client()
        # Faire un petit appel léger pour tester la connexion
        supabase.rpc('get_public_tables').execute()
        status['supabase'] = 'connected'
    except Exception as e:
        logger.error(f"ERREUR HEALTH CHECK: {str(e)}")
        status['supabase'] = 'error'
        status['supabase_error'] = str(e)
        status['status'] = 'degraded'
        
    return jsonify(status)


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    Upload d'un fichier source.
    Retourne les métadonnées (onglets pour Excel, headers).
    """
    ensure_upload_folder()
    
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier fourni'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier vide'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Type de fichier non autorisé. Formats acceptés: CSV, XLSX, XLS'}), 400
    
    # Générer un nom de fichier unique
    file_ext = file.filename.rsplit('.', 1)[1].lower()
    unique_filename = f"{uuid.uuid4()}.{file_ext}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
    
    try:
        file.save(file_path)
        
        # Charger le fichier pour extraire les métadonnées
        metadata = {
            'filename': file.filename,
            'filepath': unique_filename,
            'file_type': file_ext,
            'sheets': [],
            'headers': []
        }
        
        if file_ext in ['xlsx', 'xls']:
            # Lire les onglets Excel
            xl = pd.ExcelFile(file_path)
            metadata['sheets'] = xl.sheet_names
            
            # Charger le premier onglet par défaut
            if xl.sheet_names:
                df = pd.read_excel(file_path, sheet_name=xl.sheet_names[0], header=None)
                # Toujours convertir les colonnes en string
                df.columns = [str(c).strip() for c in df.columns]
                metadata['headers'] = list(df.columns)
                metadata['preview'] = dataframe_to_json_records(df.head(MAX_PREVIEW_ROWS))
                metadata['total_rows'] = len(df)
        
        elif file_ext == 'csv':
            # Lire le CSV de manière robuste, sans header par défaut
            df = read_csv_robust(file_path, header=None)
            df.columns = [str(c).strip() for c in df.columns]
            metadata['headers'] = list(df.columns)
            metadata['preview'] = dataframe_to_json_records(df.head(MAX_PREVIEW_ROWS))
            metadata['total_rows'] = len(df)
        
        return jsonify(metadata)
    
    except Exception as e:
        logger.error(f"ERREUR API /upload: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/preview', methods=['POST'])
def preview_file():
    """
    Retourne un aperçu du fichier avec options de configuration.
    """
    data = request.get_json()
    
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    header_row = data.get('header_row') # Nouveau: index de la ligne d'en-tête
    
    if not filename:
        return jsonify({'error': 'Nom de fichier requis'}), 400
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'Fichier non trouvé'}), 404
    
    try:
        file_ext = filename.rsplit('.', 1)[1].lower()
        
        if file_ext in ['xlsx', 'xls']:
            read_params = {'sheet_name': sheet_name} if sheet_name else {}
            if header_row is not None:
                read_params['header'] = int(header_row)
            else:
                read_params['header'] = None
            df = pd.read_excel(file_path, **read_params)
        elif file_ext == 'csv':
            read_params = {'on_bad_lines': 'skip'}
            if header_row is not None:
                read_params['header'] = int(header_row)
            else:
                read_params['header'] = None
            
            df = read_csv_robust(file_path, **read_params)
        
        # Toujours convertir les colonnes en string
        df.columns = [str(c).strip() for c in df.columns]
        
        # Normaliser les colonnes
        normalized_cols = {col: snake_case(col) for col in df.columns}
        
        return jsonify({
            'headers': list(df.columns),
            'normalized_headers': list(normalized_cols.values()),
            'original_to_normalized': normalized_cols,
            'preview': dataframe_to_json_records(df.head(MAX_PREVIEW_ROWS)),
            'total_rows': len(df),
            'total_columns': len(df.columns)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/process', methods=['POST'])
def process_file():
    """
    Traite le fichier avec les règles de normalisation configurées.
    Retourne les données prêtes pour l'insertion.
    """
    data = request.get_json()
    
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    column_types = data.get('column_types', {})
    split_datetime = data.get('split_datetime', False)
    
    if not filename:
        return jsonify({'error': 'Nom de fichier requis'}), 400
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'Fichier non trouvé'}), 404
    
    try:
        file_ext = filename.rsplit('.', 1)[1].lower()
        
        # Charger le fichier
        if file_ext in ['xlsx', 'xls']:
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(file_path)
        elif file_ext == 'csv':
            # Lire le CSV avec gestion des lignes malformées
            df = read_csv_robust(file_path)
        
        # Appliquer la normalisation
        df_normalized = normalize_dataframe(df, column_types, None, split_datetime)
        
        # Préparer les données
        records = dataframe_to_json_records(df_normalized)
        
        return jsonify({
            'processed_data': records[:MAX_PREVIEW_ROWS],  # Aperçu seulement
            'total_processed': len(df_normalized),
            'columns': list(df_normalized.columns),
            'sample': records[0] if records else None
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ROUTES API - SUPABASE
# ============================================================================

@app.route('/api/tables', methods=['GET'])
def get_tables():
    """
    Liste les tables disponibles dans Supabase.
    """
    try:
        supabase = get_supabase_client()
        
        # Utiliser la fonction RPC
        result = supabase.rpc('get_public_tables').execute()
        
        tables = [row['table_name'] for row in result.data] if result.data else []
        
        return jsonify({'tables': tables})
    
    except Exception as e:
        # Si les fonctions RPC ne sont pas encore créées, fallback
        logger.warning(f"AVERTISSEMENT API /tables: {str(e)}")
        return jsonify({
            'tables': [],
            'warning': 'Fonctions RPC non configurées. Exécutez setup_db.sql',
            'error': str(e)
        })


@app.route('/api/tables/<table_name>/columns', methods=['GET'])
def get_table_columns(table_name):
    """
    Retourne les colonnes d'une table spécifique.
    """
    try:
        supabase = get_supabase_client()
        
        # Utiliser la fonction RPC
        result = supabase.rpc('get_table_columns', {'t_name': table_name}).execute()
        
        columns = result.data if result.data else []
        
        return jsonify({
            'table_name': table_name,
            'columns': columns
        })
    
    except Exception as e:
        logger.error(f"ERREUR API /columns: {str(e)}", exc_info=True)
        return jsonify({
            'table_name': table_name,
            'columns': [],
            'warning': 'Fonctions RPC non configurées. Exécutez setup_db.sql',
            'error': str(e)
        })


@app.route('/api/import/append', methods=['POST'])
def import_append():
    """
    Insère les données dans une table existante (mode Append).
    """
    data = request.get_json()
    
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    table_name = data.get('table_name')
    column_types = data.get('column_types', {})
    column_mapping = data.get('column_mapping', {})
    split_datetime = data.get('split_datetime', False)
    header_row = data.get('header_row') # Nouveau
    
    if not all([filename, table_name]):
        return jsonify({'error': 'Paramètres requis: filename, table_name'}), 400
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'Fichier non trouvé'}), 404
    
    try:
        supabase = get_supabase_client()
        
        file_ext = filename.rsplit('.', 1)[1].lower()
        
        # Charger le fichier
        if file_ext in ['xlsx', 'xls']:
            read_params = {'sheet_name': sheet_name} if sheet_name else {}
            if header_row is not None:
                read_params['header'] = int(header_row)
            df = pd.read_excel(file_path, **read_params)
        elif file_ext == 'csv':
            read_params = {'on_bad_lines': 'skip'}
            if header_row is not None:
                read_params['header'] = int(header_row)
            
            df = read_csv_robust(file_path, **read_params)
        
        # Filtrer les lignes ignorées
        ignored_rows = data.get('ignored_rows', [])
        if ignored_rows:
            try:
                ignored_indices = [int(i) for i in ignored_rows]
                df = df.drop(index=ignored_indices, errors='ignore').reset_index(drop=True)
            except Exception as fe:
                logger.warning(f"Erreur lors du filtrage des lignes: {str(fe)}")

        # Filtrer les colonnes ignorées
        ignored_columns = data.get('ignored_columns', [])
        if ignored_columns:
             try:
                df = df.drop(columns=ignored_columns, errors='ignore')
             except Exception as ce:
                logger.warning(f"Erreur lors du filtrage des colonnes: {str(ce)}")
        
        # Toujours convertir les colonnes en string
        df.columns = [str(c).strip() for c in df.columns]
        
        # Normaliser (types, mapping, split)
        df_normalized = normalize_dataframe(df, column_types, column_mapping, split_datetime)
        
        # Convertir en records
        records = dataframe_to_json_records(df_normalized)
        
        # Insérer dans Supabase (en batches pour éviter les timeouts)
        batch_size = 1000
        total_inserted = 0
        errors = []
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                result = supabase.table(table_name).insert(batch).execute()
                # On vérifie explicitement si PostgREST retourne une erreur
                if hasattr(result, 'error') and result.error:
                    errors.append(f"Batch {i//batch_size + 1} Error: {result.error}")
                elif result.data:
                    total_inserted += len(result.data)
            except Exception as e:
                logger.error(f"Erreur d'insertion batch {i//batch_size + 1}: {str(e)}")
                errors.append(f"Batch {i//batch_size + 1} Exception: {str(e)}")
        
        return jsonify({
            'success': True,
            'table_name': table_name,
            'rows_inserted': total_inserted,
            'total_rows': len(records),
            'errors': errors if errors else None
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/import/create', methods=['POST'])
def import_create():
    """
    Crée une nouvelle table et insère les données (mode Create).
    """
    data = request.get_json()
    
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    table_name = data.get('table_name')
    column_types = data.get('column_types', {})
    column_mapping = data.get('column_mapping', {})
    split_datetime = data.get('split_datetime', False)
    ignored_rows = data.get('ignored_rows', [])
    header_row = data.get('header_row') # Nouveau
    
    if not all([filename, table_name]):
        return jsonify({'error': 'Paramètres requis: filename, table_name'}), 400
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'Fichier non trouvé'}), 404
    
    try:
        supabase = get_supabase_client()
        
        file_ext = filename.rsplit('.', 1)[1].lower()
        
        # Charger le fichier
        if file_ext in ['xlsx', 'xls']:
            read_params = {'sheet_name': sheet_name} if sheet_name else {}
            if header_row is not None:
                read_params['header'] = int(header_row)
            df = pd.read_excel(file_path, **read_params)
        elif file_ext == 'csv':
            read_params = {'on_bad_lines': 'skip'}
            if header_row is not None:
                read_params['header'] = int(header_row)
            
            df = read_csv_robust(file_path, **read_params)

        # Filtrer les lignes ignorées
        if ignored_rows:
            try:
                ignored_indices = [int(i) for i in ignored_rows]
                df = df.drop(index=ignored_indices, errors='ignore').reset_index(drop=True)
            except Exception as fe:
                logger.warning(f"Erreur lors du filtrage des lignes: {str(fe)}")
        
        # Filtrer les colonnes ignorées
        ignored_columns = data.get('ignored_columns', [])
        if ignored_columns:
             try:
                df = df.drop(columns=ignored_columns, errors='ignore')
             except Exception as ce:
                logger.warning(f"Erreur lors du filtrage des colonnes: {str(ce)}")
        
        # Toujours convertir les colonnes en string
        df.columns = [str(c).strip() for c in df.columns]
        
        # Normaliser (types, mapping, split)
        df_normalized = normalize_dataframe(df, column_types, column_mapping, split_datetime)
        
        # Générer le schéma SQL
        columns_sql = []
        for col in df_normalized.columns:
            dtype = df_normalized[col].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = 'DOUBLE PRECISION'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'TEXT'
            
            columns_sql.append(f'"{col}" {sql_type}')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
            {', '.join(columns_sql)}
        );
        """
        
        # Exécuter la création de table via RPC ou raw query
        # Note: Cela nécessite des droits suffisants
        try:
            supabase.rpc('execute_sql', {'sql': create_table_sql}).execute()
        except Exception as sql_error:
            # Si RPC execute_sql n'existe pas, on retourne le SQL à exécuter manuellement
            return jsonify({
                'warning': 'Impossible de créer la table automatiquement',
                'sql_script': create_table_sql,
                'error': str(sql_error),
                'data_preview': df_normalized.head(10).to_dict(orient='records')
            })
        
        # Insérer les données
        records = dataframe_to_json_records(df_normalized)
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                result = supabase.table(table_name).insert(batch).execute()
                if hasattr(result, 'error') and result.error:
                    raise Exception(f"Supabase Error: {result.error}")
                if result.data:
                    total_inserted += len(result.data)
            except Exception as e:
                logger.error(f"Erreur d'insertion batch {i//batch_size + 1}: {str(e)}")
                raise # On arrête tout en mode Create car la table est nouvelle
        
        return jsonify({
            'success': True,
            'table_name': table_name,
            'rows_inserted': total_inserted,
            'total_rows': len(records),
            'schema_created': True
        })
    
    except Exception as e:
        logger.error(f"ERREUR API /import/create: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ROUTES API - TEMPLATES
# ============================================================================

@app.route('/api/templates', methods=['GET'])
def list_templates():
    """
    Liste tous les templates disponibles.
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table('import_templates')\
            .select('*')\
            .order('created_at', desc=True)\
            .execute()
        
        return jsonify({'templates': result.data})
    
    except Exception as e:
        logger.error(f"ERREUR API /templates (GET): {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates', methods=['POST'])
def create_template():
    """
    Crée un nouveau template.
    """
    data = request.get_json()
    
    required_fields = ['name', 'target_table', 'column_mapping', 'column_types']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Champ requis: {field}'}), 400
    
    try:
        supabase = get_supabase_client()
        
        template_data = {
            'name': data['name'],
            'description': data.get('description', ''),
            'source_type': data.get('source_type', 'excel'),
            'target_table': data['target_table'],
            'sheet_name': data.get('sheet_name'),
            'column_mapping': data['column_mapping'],
            'column_types': data['column_types']
        }
        
        result = supabase.table('import_templates')\
            .insert(template_data)\
            .execute()
        
        return jsonify({
            'success': True,
            'template': result.data[0]
        })
    
    except Exception as e:
        logger.error(f"ERREUR API /templates (POST): {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/<template_id>', methods=['PUT'])
def update_template(template_id):
    """
    Met à jour un template existant.
    """
    data = request.get_json()
    
    try:
        supabase = get_supabase_client()
        
        update_data = {
            'name': data.get('name'),
            'description': data.get('description'),
            'target_table': data.get('target_table'),
            'sheet_name': data.get('sheet_name'),
            'column_mapping': data.get('column_mapping'),
            'column_types': data.get('column_types'),
            'updated_at': datetime.now().isoformat()
        }
        
        # Ne pas inclure les champs None
        update_data = {k: v for k, v in update_data.items() if v is not None}
        
        result = supabase.table('import_templates')\
            .update(update_data)\
            .eq('id', template_id)\
            .execute()
        
        return jsonify({
            'success': True,
            'template': result.data[0]
        })
    
    except Exception as e:
        logger.error(f"ERREUR API /templates/<id> (PUT): {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/<template_id>', methods=['DELETE'])
def delete_template(template_id):
    """
    Supprime un template.
    """
    try:
        supabase = get_supabase_client()
        
        supabase.table('import_templates')\
            .delete()\
            .eq('id', template_id)\
            .execute()
        
        return jsonify({'success': True})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/<template_id>/apply', methods=['POST'])
def apply_template(template_id):
    """
    Applique un template à un nouveau fichier.
    """
    data = request.get_json()
    
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    
    if not filename:
        return jsonify({'error': 'Nom de fichier requis'}), 400
    
    try:
        supabase = get_supabase_client()
        
        # Récupérer le template
        result = supabase.table('import_templates')\
            .select('*')\
            .eq('id', template_id)\
            .execute()
        
        if not result.data:
            return jsonify({'error': 'Template non trouvé'}), 404
        
        template = result.data[0]
        
        # Retourner la configuration du template pour l'interface
        return jsonify({
            'template': template,
            'filename': filename,
            'sheet_name': sheet_name or template.get('sheet_name'),
            'column_mapping': template['column_mapping'],
            'column_types': template['column_types'],
            'target_table': template['target_table']
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ROUTES DE NETTOYAGE
# ============================================================================

@app.route('/api/cleanup/<filename>', methods=['DELETE'])
def cleanup_file(filename):
    """
    Supprime un fichier uploadé temporairement.
    """
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Fichier non trouvé'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    # Créer le dossier uploads
    ensure_upload_folder()
    
    # Démarrer le serveur
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV', 'development') == 'development'
    
    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  Supabase Auto-Importer (RMS Sync) v2.0                         ║
║  ==========================================                      ║
║                                                                  ║
║  Serveur Flask démarré sur: http://0.0.0.0:{port}                ║
║  Mode: {'DEBUG' if debug else 'PRODUCTION'}                                        ║
║                                                                  ║
║  Endpoints principaux:                                           ║
║    - POST /api/upload          : Upload de fichier               ║
║    - POST /api/preview         : Prévisualisation                ║
║    - POST /api/process         : Traitement ETL                  ║
║    - GET  /api/tables          : Liste des tables                ║
║    - POST /api/import/append   : Insertion dans table existante  ║
║    - POST /api/import/create   : Création + insertion            ║
║    - GET  /api/templates       : Liste des templates             ║
║    - POST /api/templates       : Créer un template               ║
║                                                                  ║
║  IMPORTANT: Exécutez setup_db.sql dans Supabase Dashboard       ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    app.run(host='0.0.0.0', port=port, debug=debug)
