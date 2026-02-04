# RMS Sync v2.1 - Deployment Refresh avec Logging Robuste
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

# Configuration des handlers de logs avec fallback
def setup_logging():
    """
    Configure les handlers de logs avec une fallback intelligente.
    Essa d'utiliser un fichier de logs, sinon utilise la console.
    """
    log_dir = '/app/logs'
    log_file = os.path.join(log_dir, 'app.log')
    
    # Essayer d'utiliser le FileHandler (rotation automatique)
    try:
        # Créer le dossier de logs si possible
        os.makedirs(log_dir, exist_ok=True)
        
        # File handler avec rotation (10MB max, backup de 5 fichiers)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s] %(message)s'))
        
        # Ajouter le handler
        logger.addHandler(file_handler)
        logger.info(f"File logging activé: {log_file}")
        
    except (PermissionError, FileNotFoundError, OSError) as e:
        # CORRECTION: Si erreur de fichier, utiliser uniquement la console
        logger.warning(f"Impossible de créer le dossier de logs ({log_dir}): {str(e)}")
        logger.warning("Fallback vers logging console uniquement")
        
        # Console handler (moins verbeux)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
        
        logger.addHandler(console_handler)

# Configuration des logs au démarrage
setup_logging()

# Chargement des variables d'environnement
load_dotenv()

# Configuration Flask
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

@app.before_request
def log_request_info():
    """Logue les détails de chaque requête pour le debugging."""
    logger.debug(f"Requête {request.method} {request.path}")
    logger.debug(f"Headers: {dict(request.headers)}")
    logger.debug(f"Args: {request.args}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check pour Traefik et le monitoring."""
    try:
        # Vérifier la connexion Supabase
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        logger.info(f"Health check - Supabase URL: {supabase_url}")
        
        if not supabase_url or not supabase_key:
            logger.warning("Health check - Credentials manquantes")
            return jsonify({
                "status": "unhealthy",
                "error": "Missing SUPABASE_URL or SUPABASE_KEY",
                "timestamp": datetime.utcnow().isoformat()
            }), 503
        
        # Tester une connexion simple
        client = create_client(supabase_url, supabase_key)
        response = client.table('test').select('*').limit(1).execute()
        
        logger.info("Health check - Supabase connecté")
        
        return jsonify({
            "status": "healthy",
            "supabase": "connected",
            "version": "2.1",
            "logging": os.path.exists('/app/logs/app.log') if os.path.exists('/app/logs') else False,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check échoué: {str(e)}", exc_info=True)
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 503

@app.route('/api/diag-excel', methods=['GET'])
def diag_excel():
    """Endpoint temporaire pour inspecter la structure du fichier Planning."""
    file_path = 'MODELE DE FICHIER EXCEL/RAPPORT PLANNING D-EDGE.xlsx'
    
    if not os.path.exists(file_path):
        logger.error(f"Fichier introuvable: {file_path}")
        return jsonify({
            "error": "File not found",
            "path": file_path
        }), 404
    
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
    """Endpoint d'upload de fichiers Excel avec debug."""
    if 'file' not in request.files:
        logger.warning("Upload sans fichier")
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        logger.warning("Upload avec nom de fichier vide")
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
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
    """Endpoint de parsing de fichiers Excel avec debug."""
    if 'file' not in request.files:
        logger.warning("Parse sans fichier")
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    table_type = request.form.get('type', 'dedge_planning')
    hotel_id = request.form.get('hotel_id', 'default')
    
    if file:
        try:
            logger.info(f"Parse demandé: type={table_type}, hotel_id={hotel_id}")
            
            # Initialiser le processeur approprié
            processor = ProcessorFactory.get_processor(table_type, file, hotel_id)
            
            # Transformer les données
            processor.apply_transformations()
            
            # Pousser vers Supabase avec gestion d'erreurs
            result = processor.push_to_supabase()
            records_inserted = result['success']
            failed_chunks = result['failed']
            
            logger.info(f"Insertion terminée: {records_inserted} enregistrements, {len(failed_chunks)} chunks échoués")
            
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

@app.errorhandler(404)
def handle_not_found(e):
    """Gestionnaire d'erreurs 404 - Resource not found."""
    logger.warning(f"404 Not Found: {request.path}")
    return jsonify({
        "error": "Resource not found",
        "path": request.path,
        "message": "The requested resource was not found on this server.",
        "timestamp": datetime.utcnow().isoformat()
    }), 404

@app.errorhandler(405)
def handle_method_not_allowed(e):
    """Gestionnaire d'erreurs 405 - Method not allowed."""
    logger.warning(f"405 Method Not Allowed: {request.method} {request.path}")
    return jsonify({
        "error": "Method not allowed",
        "method": request.method,
        "path": request.path,
        "timestamp": datetime.utcnow().isoformat()
    }), 405

@app.errorhandler(500)
def handle_internal_server_error(e):
    """Gestionnaire d'erreurs 500 - Internal server error."""
    logger.error(f"500 Internal Server Error: {str(e)}", exc_info=True)
    return jsonify({
        "error": "Internal server error",
        "message": "An unexpected error occurred. Please try again later.",
        "timestamp": datetime.utcnow().isoformat()
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Gestionnaire d'erreurs global pour toutes les exceptions non gérées."""
    logger.error(f"Erreur non gérée: {str(e)}", exc_info=True)
    
    # Ne pas renvoyer le traceback complet en production pour la sécurité
    is_debug = os.getenv('FLASK_ENV') == 'development'
    
    return jsonify({
        "error": str(e),
        "type": type(e).__name__,
        "message": "An unexpected error occurred. The error has been logged.",
        "timestamp": datetime.utcnow().isoformat(),
        "debug_info": str(e) if is_debug else None
    }), 500

if __name__ == '__main__':
    # S'assurer que le dossier d'upload existe
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    logger.info("Application démarrée")
    logger.info(f"Environment: {os.getenv('FLASK_ENV', 'production')}")
    logger.info(f"Python Version: {sys.version}")
    
    # En production, utiliser Gunicorn
    if os.getenv('FLASK_ENV') == 'production':
        logger.info("Mode production: Gunicorn détecté")
        # CORRECTION: Utiliser 2 workers au lieu de 4 (plus stable)
        # CORRECTION: Supprimer --preload (réduit la consommation mémoire)
        # --timeout 300: On laisse 5 minutes pour le démarrage (cold start) et les gros fichiers
        CMD = ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "300", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
    else:
        logger.info("Mode développement: Flask debug activé")
        app.run(host='0.0.0.0', port=5000, debug=True)
