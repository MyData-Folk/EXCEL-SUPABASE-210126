FROM python:3.12-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Création du dossier uploads avec les bonnes permissions
RUN mkdir -p uploads && chmod 777 uploads

EXPOSE 5000

# Utilisation de Gunicorn (4 workers pour la performance)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "120", "app:app"]