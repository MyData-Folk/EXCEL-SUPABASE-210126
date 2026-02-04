# RMS Sync v2.1 - Full Stack with Global APP_DIR (Fixes NameError)
import os
import sys
import json
import csv
import math
import uuid
import re
import logging
from datetime import datetime
from pathlib import Path
from functools import wraps
import traceback

import pandas as pd
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from supabase import create_client, Client

# S'assurer que le dossier courant est accessible
sys.path.append(os.getcwd())
from utils import snake_case, json_safe
from processor import ProcessorFactory

# Configuration des logs
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# ============================================================
# DÉFINITION GLOBALE DES RÉPERTOIRES
# ============================================================
# CORRECTION : Définir APP_DIR et UPLOAD_DIR au début du fichier
# pour qu'ils soient accessibles dans les gestionnaires d'erreurs
APP_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(APP_DIR, 'uploads')

# ============================================================
# SETUP LOGGING
# ============================================================

def setup_logging():
    log_dir = '/app/logs'
    log_file = os.path.join(log_dir, 'app.log')
    
    try:
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s] %(message)s'))
        
        logger.addHandler(file_handler)
        logger.info(f"File logging activé: {log_file}")
        
    except (PermissionError, FileNotFoundError, OSError) as e:
        logger.warning(f"Impossible de créer le dossier de logs ({log_dir}): {str(e)}")
        logger.warning("Fallback vers logging console uniquement")
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
        
        logger.addHandler(console_handler)

setup_logging()

# ============================================================
# CONFIGURATION FLASK
# ============================================================

load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH',52428800))
app.config['UPLOAD_FOLDER'] = UPLOAD_DIR

CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

@app.before_request
def log_request_info():
    """Logue les détails de chaque requête."""
    logger.debug(f"Requête {request.method} {request.path}")
    logger.debug(f"Headers: {dict(request.headers)}")
    logger.debug(f"Args: {request.args}")

# ============================================================
# ROUTE FRONTEND (SERVEUR DE FICHIERS STATIQUES)
# ============================================================

@app.route('/')
def index():
    """Page d'accueil (Dashboard) - Utilise APP_DIR global."""
    index_path = os.path.join(APP_DIR, 'index.html')
    
    try:
        logger.info(f"Serving index.html from: {index_path}")
        return send_file(index_path)
    except FileNotFoundError:
        logger.error(f"index.html not found at: {index_path}")
        return jsonify({
            "error": "index.html not found",
            "path": index_path,
            "app_dir": APP_DIR,
            "message": "The frontend file could not be found on the server."
        }), 404

@app.route('/favicon.ico')
def favicon():
    """Icône du navigateur."""
    favicon_path = os.path.join(APP_DIR, 'favicon.ico')
    
    try:
        return send_file(favicon_path, mimetype='image/vnd.microsoft.icon')
    except FileNotFoundError:
        return '', 204

# ============================================================
# ROUTES API (BACKEND)
# ============================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check simplifié - Vérifie seulement si le client peut être initialisé."""
    try:
        # 1. Vérifier si les variables sont définies
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            return jsonify({
                "status": "unhealthy",
                "error": "Missing SUPABASE_URL or SUPABASE_KEY",
                "supabase_connected": False,
                "timestamp": datetime.utcnow().isoformat()
            }), 503
        
        # 2. Initialiser le client (Ceci teste la connexion et les credentials)
        client = create_client(supabase_url, supabase_key)
        
        # 3. Succès immédiat (on n'a pas besoin de faire une requête SQL)
        logger.info(f"Health check OK - Supabase connected: {supabase_url}")
        
        return jsonify({
            "status": "healthy",
            "supabase_connected": True,
            "supabase_url": supabase_url,
            "version": "2.1-fullstack",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Health check échoué: {str(e)}", exc_info=True)
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "supabase_connected": False,
            "timestamp": datetime.utcnow().isoformat()
        }), 503

@app.route('/api/diag-excel', methods=['GET'])
def diag_excel():
    """Endpoint temporaire pour inspecter la structure du fichier Planning."""
    file_path = os.path.join(APP_DIR, 'MODELE DE FICHIER EXCEL', 'RAPPORT PLANNING D-EDGE.xlsx')
    
    if not os.path.exists(file_path):
        logger.error(f"Fichier introuvable: {file_path}")
        return jsonify({"error": "File not found", "path": file_path}), 404
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Fichier lu: {len(df)} lignes, {len(df.columns)} colonnes")
        
        return jsonify({
            "path": file_path,
            "shape": df.shape,
            "columns": list(df.columns),
            "memory_usage": df.memory_usage(deep=True).to_dict()
        }), 200
    except Exception as e:
        logger.error(f"Erreur lecture Excel: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "path": file_path
        }), 500

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Endpoint d'upload de fichiers Excel."""
    if 'file' not in request.files:
        logger.warning("Upload sans fichier")
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        logger.warning("Upload avec nom de fichier vide")
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_DIR, filename)
        
        logger.info(f"Upload démarré: {filename} -> {filepath}")
        
        try:
            file.save(filepath)
            file_size = os.path.getsize(filepath)
            logger.info(f"Upload terminé: {file_size} bytes")
            
            return jsonify({
                "message": "File uploaded successfully",
                "filename": filename,
                "path": filepath,
                "size": file_size,
                "timestamp": datetime.utcnow().isoformat()
            }), 201
        except Exception as e:
            logger.error(f"Erreur upload: {str(e)}", exc_info=True)
            return jsonify({
                "error": str(e),
                "filename": filename
            }), 500
    else:
        return jsonify({"error": "Upload failed"}), 500

@app.route('/api/parse', methods=['POST'])
def parse_file():
    """Endpoint de parsing de fichiers Excel."""
    if 'file' not in request.files:
        logger.warning("Parse sans fichier")
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    table_type = request.form.get('type', 'dedge_planning')
    hotel_id = request.form.get('hotel_id', 'default')
    
    if file:
        try:
            logger.info(f"Parse demandé: type={table_type}, hotel_id={hotel_id}")
            
            processor = ProcessorFactory.get_processor(table_type, file, hotel_id)
            processor.apply_transformations()
            
            result = processor.push_to_supabase()
            records_inserted = result['success']
            failed_chunks = result['failed']
            
            return jsonify({
                "message": "File parsed and uploaded successfully",
                "table_type": table_type,
                "records_inserted": records_inserted,
                "hotel_id": hotel_id,
                "failed_chunks": len(failed_chunks),
                "timestamp": datetime.utcnow().isoformat()
            }), 200
        except ValueError as e:
            logger.error(f"Erreur validation: {str(e)}", exc_info=True)
            return jsonify({
                "error": str(e),
                "type": "ValidationError"
            }), 400
        except Exception as e:
            logger.error(f"Erreur parsing: {str(e)}", exc_info=True)
            return jsonify({
                "error": str(e),
                "type": "ParsingError"
            }), 500

# ============================================================
# GESTIONNAIRES D'ERREURS (Utilise APP_DIR global)
# ============================================================

@app.errorhandler(404)
def handle_not_found(e):
    """Gestionnaire d'erreurs 404."""
    return jsonify({
        "error": "Resource not found",
        "path": request.path,
        "message": "The requested resource was not found on this server.",
        "app_dir": APP_DIR,  # Utilise la variable globale
        "timestamp": datetime.utcnow().isoformat()
    }), 404

@app.errorhandler(500)
def handle_internal_server_error(e):
    """Gestionnaire d'erreurs 500."""
    logger.error(f"500 Internal Server Error: {str(e)}", exc_info=True)
    return jsonify({
        "error": "Internal server error",
        "message": "An unexpected error occurred. Please try again later.",
        "timestamp": datetime.utcnow().isoformat()
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Gestionnaire d'erreurs global."""
    logger.error(f"Erreur non gérée: {str(e)}", exc_info=True)
    is_debug = os.getenv('FLASK_ENV') == 'development'
    
    return jsonify({
        "error": str(e),
        "type": type(e).__name__,
        "message": "An unexpected error occurred. The error has been logged.",
        "timestamp": datetime.utcnow().isoformat(),
        "debug_info": str(e) if is_debug else None
    }), 500

# ============================================================
# MAIN ENTRY POINT
# ============================================================

if __name__ == '__main__':
    # Création des dossiers nécessaires (APP_DIR et UPLOAD_DIR sont déjà définis)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs('/app/logs', exist_ok=True)
    
    logger.info("Application démarrée (Full Stack with Global APP_DIR)")
    logger.info(f"Environment: {os.getenv('FLASK_ENV', 'production')}")
    logger.info(f"App Directory: {APP_DIR}")
    logger.info(f"Upload Directory: {UPLOAD_DIR}")
    logger.info(f"Python Version: {sys.version.split()[0]}")
    logger.info(f"index.html exists: {os.path.exists(os.path.join(APP_DIR, 'index.html'))}")
    
    if os.getenv('FLASK_ENV') == 'production':
        logger.info("Mode production: Gunicorn détecté")
        # CMD configuré dans Dockerfile
    else:
        logger.info("Mode développement: Flask debug activé")
        app.run(host='0.0.0.0', port=5000, debug=True)
