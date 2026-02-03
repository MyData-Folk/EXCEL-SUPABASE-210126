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
# UTILS: ENV VARS ROBUSTES
# ============================================================
def get_env_flexible(name, default=None):
    """Cherche une variable d'environnement de manière insensible à la casse."""
    # 1. Exact match
    val = os.getenv(name)
    if val: return val
    # 2. Case variations common
    val = os.getenv(name.lower()) or os.getenv(name.upper())
    if val: return val
    # 3. Scan complet
    for k, v in os.environ.items():
        if k.upper() == name.upper():
            return v
    return default

# ============================================================
# CONFIGURATION FLASK
# ============================================================
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH', 52428800))
app.config['UPLOAD_FOLDER'] = UPLOAD_DIR

CORS(app, resources={r"/api/*": {"origins": "*"}})

def get_supabase_client() -> Client:
    # Récupération ultra-robuste
    url = get_env_flexible('SUPABASE_URL')
    key = get_env_flexible('SUPABASE_KEY')
    
    if not url or not key:
        msg = f"CONFIG ERROR: SUPABASE_URL ({'OK' if url else 'MISSING'}) or SUPABASE_KEY ({'OK' if key else 'MISSING'}) are not set."
        logger.critical(msg)
        raise ValueError(msg)
    return create_client(url, key)


def resolve_hotel_id(supabase: Client, hotel_code: str = None, hotel_id: str = None):
    if hotel_code:
        try:
            result = supabase.table('hotels').select('id').eq('code', hotel_code).limit(1).execute()
            if result.data:
                return result.data[0]['id']
        except Exception as e:
            logger.warning(f"Hotel lookup via code failed: {e}")
        result = supabase.table('hotels').select('id').eq('hotel_id', hotel_code).limit(1).execute()
        if result.data:
            return result.data[0]['id']
        raise ValueError(f"Hotel code introuvable: {hotel_code}")
    if hotel_id:
        return hotel_id
    raise ValueError("hotel_code ou hotel_id requis")

# ============================================================
# ROUTES DIAGNOSTIC
# ============================================================
@app.route('/api/debug/env', methods=['GET'])
def debug_env():
    """Liste les clés des variables d'env disponibles et test la connexion Supabase."""
    supabase_url = get_env_flexible('SUPABASE_URL')
    supabase_key = get_env_flexible('SUPABASE_KEY')
    
    connection_test = "Not Attempted"
    if supabase_url and supabase_key:
        try:
            supabase = create_client(supabase_url, supabase_key)
            # Tentative de lecture simple pour tester la connexion réelle
            res = supabase.table('hotels').select('count', count='exact').limit(1).execute()
            connection_test = "SUCCESS" if res else "No Data"
        except Exception as e:
            connection_test = f"FAILED: {str(e)}"

    return jsonify({
        "keys": sorted(list(os.environ.keys())),
        "supabase_url_found": supabase_url is not None,
        "supabase_key_found": supabase_key is not None,
        "supabase_connection": connection_test,
        "python_version": sys.version
    })

# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================
@app.errorhandler(Exception)
def handle_exception(e):
    # Log l'exception avec traceback complet
    logger.error(f"UNHANDLED EXCEPTION: {str(e)}\n{traceback.format_exc()}")
    return jsonify({
        "error": "Internal Server Error",
        "message": str(e),
        "trace": traceback.format_exc()
    }), 500

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
    filename = data.get('filename')
    category = data.get('category') or data.get('report_type')
    hotel_code = data.get('hotel_code')
    hotel_id = data.get('hotel_id')
    tab_name = data.get('tab_name')
    
    filepath = os.path.join(UPLOAD_DIR, filename)
    
    logger.info(f"AUTO-PROCESS: file={filename}, cat={category}, hotel={hotel_id or hotel_code}, tab={tab_name}")
    
    try:
        supabase = get_supabase_client()
        resolved_hotel_id = resolve_hotel_id(supabase, hotel_code=hotel_code, hotel_id=hotel_id)
        processor = ProcessorFactory.get_processor(category, filepath, resolved_hotel_id, supabase, tab_name=tab_name)
        processor.apply_transformations()
        res = processor.push_to_supabase()
        if isinstance(res, dict):
            if 'success' in res:
                rows_inserted = res['success']
            else:
                rows_inserted = sum(
                    value if isinstance(value, int) else value.get('success', 0)
                    for value in res.values()
                )
        else:
            rows_inserted = res
        return jsonify({
            'success': True,
            'rows_inserted': rows_inserted,
            'target_table': processor.target_table,
            'result': res
        })
    except Exception as e:
        logger.error(f"AUTO-PROCESS FAILURE: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'error': str(e),
            'trace': traceback.format_exc() if app.debug else None
        }), 500
# ============================================================
# ROUTES TEMPLATES
# ============================================================
@app.route('/api/templates', methods=['GET'])
def list_templates():
    try:
        supabase = get_supabase_client()
        result = supabase.table('import_templates').select('*').order('created_at', desc=True).execute()
        return jsonify({'templates': result.data})
    except Exception as e:
        logger.error(f"ERREUR GET /api/templates: {e}")
        if 'import_templates' in str(e):
            return jsonify({'templates': [], 'warning': 'Table import_templates introuvable'}), 200
        return jsonify({'error': str(e)}), 500

@app.route('/api/templates', methods=['POST'])
def create_template():
    data = request.get_json()
    try:
        supabase = get_supabase_client()
        result = supabase.table('import_templates').insert(data).execute()
        return jsonify({'success': True, 'template': result.data[0]})
    except Exception as e:
        logger.error(f"ERREUR POST /api/templates: {e}")
        if 'import_templates' in str(e):
            return jsonify({'error': 'Table import_templates introuvable'}), 400
        return jsonify({'error': str(e)}), 500

@app.route('/api/templates/<template_id>', methods=['PUT'])
def update_template(template_id):
    data = request.get_json()
    try:
        supabase = get_supabase_client()
        result = supabase.table('import_templates').update(data).eq('id', template_id).execute()
        return jsonify({'success': True, 'template': result.data[0]})
    except Exception as e:
        logger.error(f"ERREUR PUT /api/templates: {e}")
        if 'import_templates' in str(e):
            return jsonify({'error': 'Table import_templates introuvable'}), 400
        return jsonify({'error': str(e)}), 500

@app.route('/api/templates/<template_id>', methods=['DELETE'])
def delete_template(template_id):
    try:
        supabase = get_supabase_client()
        supabase.table('import_templates').delete().eq('id', template_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        if 'import_templates' in str(e):
            return jsonify({'error': 'Table import_templates introuvable'}), 400
        return jsonify({'error': str(e)}), 500

@app.route('/api/templates/<template_id>/apply', methods=['POST'])
def apply_template(template_id):
    data = request.get_json()
    try:
        supabase = get_supabase_client()
        result = supabase.table('import_templates').select('*').eq('id', template_id).execute()
        if not result.data: return jsonify({'error': 'Template non trouvé'}), 404
        template = result.data[0]
        return jsonify({
            'template': template,
            'filename': data.get('filename'),
            'sheet_name': data.get('sheet_name') or template.get('sheet_name'),
            'column_mapping': template['column_mapping'],
            'column_types': template['column_types'],
            'target_table': template['target_table']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# ROUTES HOTELS
# ============================================================
@app.route('/api/hotels', methods=['GET'])
def get_hotels():
    try:
        logger.info("Tentative de récupération de la liste des hôtels...")
        supabase = get_supabase_client()
        try:
            result = supabase.table('hotels').select('*').order('name').execute()
            data = result.data or []
            if data:
                logger.info(f"Liste des hôtels récupérée: {len(data)} hôtels trouvés.")
                return jsonify({'hotels': data})
        except Exception as inner_error:
            logger.warning(f"Fallback hotels query: {inner_error}")
        result = supabase.table('hotels').select('*').execute()
        data = result.data or []
        logger.info(f"Liste des hôtels récupérée: {len(data)} hôtels trouvés.")
        return jsonify({'hotels': data})
    except Exception as e:
        error_detail = traceback.format_exc()
        logger.error(f"ERREUR CRITIQUE GET /api/hotels: {str(e)}\n{error_detail}")
        return jsonify({
            'error': str(e),
            'message': "Erreur lors de la récupération des hôtels. Vérifiez les credentials Supabase.",
            'trace': error_detail 
        }), 500

@app.route('/api/hotels', methods=['POST'])
def create_hotel():
    data = request.get_json()
    code = data.get('hotel_id') or data.get('code')
    name = data.get('hotel_name') or data.get('name')
    if not code or not name:
        return jsonify({'error': 'code et name sont requis'}), 400
    try:
        supabase = get_supabase_client()
        result = supabase.table('hotels').insert({'code': code, 'name': name}).execute()
        if result.data:
            return jsonify({'success': True, 'hotel': result.data[0]})
        result = supabase.table('hotels').insert({'hotel_id': code, 'hotel_name': name}).execute()
        return jsonify({'success': True, 'hotel': result.data[0]})
    except Exception as e:
        logger.error(f"ERREUR POST /api/hotels: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/hotels/<id>', methods=['DELETE'])
def delete_hotel(id):
    try:
        supabase = get_supabase_client()
        supabase.table('hotels').delete().eq('id', id).execute()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"ERREUR DELETE /api/hotels: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================
# CLEANUP
# ============================================================
@app.route('/api/cleanup/<filename>', methods=['DELETE'])
def cleanup_file(filename):
    file_path = os.path.join(UPLOAD_DIR, filename)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({'success': True})
        return jsonify({'error': 'Fichier non trouvé'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# CACHE & FILES
# ============================================================
@app.route('/api/cache', methods=['GET'])
def list_cache():
    ensure_upload_folder()
    try:
        files = []
        for f in sorted(os.listdir(UPLOAD_DIR)):
            path = os.path.join(UPLOAD_DIR, f)
            if os.path.isfile(path):
                s = os.stat(path)
                files.append({'filename': f, 'size': s.st_size, 'created_at': datetime.fromtimestamp(s.st_ctime).isoformat()})
        return jsonify({'files': sorted(files, key=lambda x: x['created_at'], reverse=True)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

if __name__ == '__main__':
    ensure_upload_folder()
    app.run(host='0.0.0.0', port=5000, debug=True)
