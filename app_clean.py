import os
import sys
import json
import csv
import io
import math
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

# S'assurer que le dossier courant est accessible
sys.path.append(os.getcwd())
from utils import snake_case
from processor import ProcessorFactory

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

# Configuration Flask
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_CONTENT_LENGTH', 52428800))
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', './uploads')

# Configuration CORS
CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.errorhandler(Exception)
def handle_exception(e):
    msg = str(e)
    logger.error(f"ERREUR GLOBAL: {msg}", exc_info=True)
    return jsonify({"error": "Internal Server Error", "message": msg}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "version": "2.1"})

@app.route('/api/debug', methods=['GET'])
def diagnostic_debug():
    return jsonify({
        "status": "ok",
        "python": sys.version,
        "cwd": os.getcwd()
    })

# --- RESTE DU CODE (S'ASSURER DE COPIER LES ROUTES UTILES) ---
# [Note: Je vais utiliser replace_file_content pour garder les routes, mais je nettoie le haut]
