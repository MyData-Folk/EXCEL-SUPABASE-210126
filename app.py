# RMS Sync v2.1 - Full Stack with Global APP_DIR
import os
import sys
import json
import csv
import math
import uuid
import re
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from functools import wraps
import traceback
import unicodedata

import pandas as pd
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from supabase import create_client, Client
from werkzeug.utils import secure_filename

# S'assurer que le dossier courant est accessible
sys.path.append(os.getcwd())
try:
    from utils import snake_case, json_safe
except ImportError:
    # Fallback si utils.py n'est pas encore prêt ou différent
    def snake_case(s): return str(s).lower().replace(' ', '_')
    def json_safe(o): return o

from processor import ProcessorFactory

# Configuration des logs
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# ============================================================
# DÉFINITION GLOBALE DES RÉPERTOIRES
# ============================================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(APP_DIR, 'uploads')
MAX_PREVIEW_ROWS = 20

# ============================================================
# SETUP LOGGING
# ============================================================
def setup_logging():
    log_dir = os.path.join(APP_DIR, 'logs')
    log_file = os.path.join(log_dir, 'app.log')
    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s] %(message)s'))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Erreur logging: {e}")

setup_logging()

# ============================================================
# CONFIGURATION FLASK
# ============================================================
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH', 52428800))
app.config['UPLOAD_FOLDER'] = UPLOAD_DIR

CORS(app, resources={r"/api/*": {"origins": "*"}})

def get_supabase_client() -> Client:
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY not set")
    return create_client(url, key)

# ============================================================
# UTILS & HELPERS
# ============================================================
def ensure_upload_folder():
    if not os.path.exists(UPLOAD_DIR):
        os.makedirs(UPLOAD_DIR, exist_ok=True)

def read_csv_robust(file_path, **kwargs):
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    delimiters = [None, ',', ';', '\t', '|']
    read_params = kwargs.copy()
    read_params['on_bad_lines'] = 'skip'
    read_params['engine'] = 'python'
    
    last_error = None
    for encoding in encodings:
        read_params['encoding'] = encoding
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                sample = f.read(2048)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    detected_sep = dialect.delimiter
                except:
                    detected_sep = None
            
            seps = [detected_sep] if detected_sep else delimiters[1:]
            for sep in seps:
                if sep: read_params['sep'] = sep
                try:
                    df = pd.read_csv(file_path, **read_params)
                    if not df.empty: return df
                except Exception as e:
                    last_error = e
        except:
            continue
    raise last_error or ValueError("Impossible de lire le CSV")

def parse_date(value):
    if pd.isna(value) or value == '': return None
    try:
        return pd.to_datetime(value, dayfirst=True).strftime('%Y-%m-%d')
    except:
        return str(value)

def clean_number(value):
    if pd.isna(value): return None
    try:
        s = str(value).replace(' ', '').replace('\xa0', '').replace(',', '.')
        s = re.sub(r'[^0-9.\-]', '', s)
        return float(s)
    except:
        return None

def clean_text(value):
    if pd.isna(value): return None
    return str(value).strip()

def normalize_dataframe(df, column_types=None, column_mapping=None, split_datetime=False, hotel_id=None):
    df = df.copy()
    if column_types:
        for col, t in column_types.items():
            if col not in df.columns: continue
            if t == 'date': df[col] = df[col].apply(parse_date)
            elif t == 'numeric': df[col] = df[col].apply(clean_number)
            elif t == 'text': df[col] = df[col].apply(clean_text)
    
    if column_mapping:
        valid = {k: v for k, v in column_mapping.items() if v and k in df.columns}
        if valid: df = df[list(valid.keys())].rename(columns=valid)
        else: df.columns = [snake_case(c) for c in df.columns]
    else:
        df.columns = [snake_case(c) for c in df.columns]
        
    if hotel_id:
        df.insert(0, 'hotel_id', hotel_id)
    return df.where(pd.notnull(df), None)

def dataframe_to_json_records(df):
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient='records')
    for r in records:
        for k, v in r.items():
            if isinstance(v, (datetime, pd.Timestamp)):
                r[k] = v.strftime('%Y-%m-%d')
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                r[k] = None
    return records

# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    return send_file(os.path.join(APP_DIR, 'index.html'))

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "version": "2.1-merged"})

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files: return jsonify({"error": "No file"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No name"}), 400
    
    ensure_upload_folder()
    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_DIR, filename)
    file.save(filepath)
    
    file_ext = filename.rsplit('.', 1)[1].lower()
    metadata = {'filename': filename, 'filepath': filename}
    
    try:
        if file_ext in ['xlsx', 'xls']:
            xl = pd.ExcelFile(filepath)
            metadata['sheets'] = xl.sheet_names
            df = pd.read_excel(filepath, sheet_name=xl.sheet_names[0], header=None)
        else:
            df = read_csv_robust(filepath, header=None)
        
        df.columns = [str(c).strip() for c in df.columns]
        metadata['headers'] = list(df.columns)
        metadata['preview'] = dataframe_to_json_records(df.head(MAX_PREVIEW_ROWS))
        metadata['total_rows'] = len(df)
        return jsonify(metadata)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/preview', methods=['POST'])
def preview_file():
    data = request.get_json()
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    header_row = data.get('header_row')
    
    filepath = os.path.join(UPLOAD_DIR, filename)
    file_ext = filename.rsplit('.', 1)[1].lower()
    
    try:
        params = {'sheet_name': sheet_name} if sheet_name else {}
        params['header'] = int(header_row) if header_row is not None else None
        
        if file_ext in ['xlsx', 'xls']:
            df = pd.read_excel(filepath, **params)
        else:
            df = read_csv_robust(filepath, **params)
            
        df.columns = [str(c).strip() for c in df.columns]
        return jsonify({
            'headers': list(df.columns),
            'preview': dataframe_to_json_records(df.head(MAX_PREVIEW_ROWS)),
            'total_rows': len(df)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['GET'])
def get_tables():
    try:
        result = get_supabase_client().rpc('get_public_tables').execute()
        return jsonify({'tables': [r['table_name'] for r in result.data] if result.data else []})
    except Exception as e:
        return jsonify({'tables': [], 'error': str(e)})

@app.route('/api/tables/<table_name>/columns', methods=['GET'])
def get_table_columns(table_name):
    try:
        result = get_supabase_client().rpc('get_table_columns', {'t_name': table_name}).execute()
        return jsonify({'table_name': table_name, 'columns': result.data or []})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/import/append', methods=['POST'])
def import_append():
    data = request.get_json()
    filepath = os.path.join(UPLOAD_DIR, data['filename'])
    try:
        df = pd.read_excel(filepath) if data['filename'].endswith('.xlsx') else read_csv_robust(filepath)
        df_norm = normalize_dataframe(df, data.get('column_types'), data.get('column_mapping'), False, data.get('hotel_id'))
        records = dataframe_to_json_records(df_norm)
        
        supabase = get_supabase_client()
        batch_size = 500
        for i in range(0, len(records), batch_size):
            supabase.table(data['table_name']).insert(records[i:i+batch_size]).execute()
            
        return jsonify({'success': True, 'rows_inserted': len(records)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/auto-process', methods=['POST'])
def auto_process():
    data = request.get_json()
    filepath = os.path.join(UPLOAD_DIR, data['filename'])
    try:
        processor = ProcessorFactory.get_processor(data['category'], filepath, data['hotel_id'], get_supabase_client())
        processor.apply_transformations()
        res = processor.push_to_supabase()
        return jsonify({'success': True, 'rows_inserted': res['success'], 'target_table': processor.target_table})
    except Exception as e:
        logger.error(f"Auto-process error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    ensure_upload_folder()
    app.run(host='0.0.0.0', port=5000, debug=True)
