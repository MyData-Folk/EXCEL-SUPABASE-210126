# Cahier des Charges - RMS Sync v2.0 (Supabase Auto-Importer)

## 1. Présentation du Projet
### 1.1 Contexte
L'application **RMS Sync v2.0** est un outil d'automatisation ETL (Extract, Transform, Load) conçu pour les hôteliers. Son but principal est de simplifier l'importation de données complexes provenant de fichiers Excel (rapports D-EDGE, OTA Insight, etc.) vers une base de données **Supabase** (PostgreSQL).

### 1.2 Objectif
Automatiser le nettoyage, la transformation et la synchronisation des données hôtelières pour alimenter des tableaux de bord de Revenue Management System (RMS).

---

## 2. Architecture Technique
### 2.1 Stack Technologique
- **Frontend** : HTML5, Vanilla JavaScript, CSS3 (Design moderne et responsive).
- **Backend** : Python 3.10+ avec Flask.
- **Manipulation de données** : Pandas (pour le traitement Excel/CSV complexe).
- **Base de données** : Supabase (PostgreSQL) avec Row Level Security (RLS).
- **Conteneurisation** : Docker & Docker Compose.
- **Serveur Web** : Nginx (Reverse Proxy).

### 2.2 Infrastructure & URLs
- **Admin** : `https://admin.hotelmanager.fr`
- **API** : `api.hotelmanager.fr`
- **Frontend** : `https://hotel.hotelmanager.fr`

---

## 3. Spécifications Fonctionnelles
### 3.1 Gestion des Fichiers
- **Upload multi-format** : Support des fichiers `.csv`, `.xlsx`, `.xls`.
- **Aperçu robuste** : Prévisualisation des 10 premières lignes avant import.
- **Gestion des onglets** : Capacité à choisir un onglet spécifique dans un fichier Excel.

### 3.2 Moteur de Transformation (Processor)
L'intelligence de l'application réside dans `processor.py` qui gère des types de rapports spécifiques :
- **D-EDGE Planning** : Transformation de tableaux croisés (unpivot) en format liste plate.
- **D-EDGE Réservations** : Normalisation automatique des dates et injection de l'ID hôtel.
- **OTA Insight** : Mapping intelligent des colonnes et nettoyage des en-têtes.
- **Salons & Événements** : Normalisation des calendriers événementiels.

### 3.3 Intégration Supabase
- **Mode "Create"** : Création automatique de tables SQL à partir de la structure du fichier Excel.
- **Mode "Append"** : Insertion de données dans des tables existantes avec mapping de colonnes.
- **Injection automatique** : Ajout systématique d'une colonne `hotel_id` pour la segmentation des données.

---

## 4. Spécifications Techniques
### 4.1 Sécurité
- Utilisation de `execute_sql` via une fonction RPC PostgreSQL avec `SECURITY DEFINER` pour les opérations de structure.
- RLS (Row Level Security) activé sur les tables critiques (`hotels`, `import_templates`).

### 4.2 Nettoyage de Données
- **Dates** : Conversion intelligente des formats Excel (serial), ISO et FR (JJ/MM/AAAA) vers le format SQL `YYYY-MM-DD`.
- **Nombres** : Nettoyage des symboles monétaires, espaces et séparateurs de milliers (FR/EN).
- **Texte** : Normalisation en `snake_case` pour les noms de colonnes SQL.

---

## 5. Maintenance et Évolutivité
### 5.1 Templates d'Import
L'application permet de sauvegarder des "Templates" de mapping pour réutiliser les configurations lors d'imports futurs (Table `import_templates`).

### 5.2 Déploiement
Le projet est prêt pour la production avec Docker :
- `Dockerfile` pour le backend Flask.
- `docker-compose.yml` pour orchestrer Flask et Nginx.
