# 1. Image de base stable
FROM python:3.12-slim

# 2. Variables d'environnement pour Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production

# 3. Répertoire de travail
WORKDIR /app

# 4. Installation des dépendances système nécessaires
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 5. Mise à jour de PIP
RUN pip install --no-cache-dir --upgrade pip

# 6. Installation FORCÉE des dépendances
RUN pip install --no-cache-dir \
    Flask==3.0.2 \
    Flask-Cors==4.0.0 \
    gunicorn==21.2.0 \
    pandas==2.2.0 \
    numpy==1.26.4 \
    openpyxl==3.1.2 \
    xlrd==2.0.1 \
    supabase==2.27.2 \
    python-dotenv==1.0.1 \
    python-dateutil==2.8.2 \
    chardet==5.2.0

# 7. Copie du code source
COPY . .

# 8. Préparation du dossier de stockage des fichiers
RUN mkdir -p /app/uploads && chmod 777 /app/uploads

# 9. Exposition du port utilisé par Flask/Gunicorn
EXPOSE 5000

# 10. Commande de lancement avec Gunicorn
# --workers 4 : Plus de parallélisme
# --timeout 300 : On laisse 5 minutes pour le démarrage (cold start) et les gros fichiers
# --preload : Charge l'app AVANT de forker les workers pour voir les erreurs de boot immédiatement
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "300", "--preload", "--access-logfile", "-", "--error-logfile", "-", "app:app"]