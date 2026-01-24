import pandas as pd
import os
import json
import logging
import math
from datetime import datetime
from supabase import create_client, Client
from utils import snake_case, json_safe

logger = logging.getLogger(__name__)

class BaseProcessor:
    """Classe de base pour tous les processeurs Excel."""
    
    def __init__(self, file_path, hotel_id, supabase_client: Client):
        self.file_path = file_path
        self.hotel_id = hotel_id
        self.supabase = supabase_client
        self.df = None
        self.target_table = None

    def read_excel(self, sheet_name=0, **kwargs):
        """Lit le fichier Excel."""
        try:
            self.df = pd.read_excel(self.file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Fichier lu: {len(self.df)} lignes, {len(self.df.columns)} colonnes")
            return True
        except Exception as e:
            logger.error(f"Erreur lecture Excel: {str(e)}")
            raise e

    def inject_hotel_id(self):
        """Injecte la colonne hotel_id."""
        if self.df is not None and 'hotel_id' not in self.df.columns:
            self.df['hotel_id'] = self.hotel_id

    def normalize_dates(self, date_columns):
        """Normalise les colonnes de date au format YYYY-MM-DD."""
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    def push_to_supabase(self):
        """Pousse les données vers Supabase avec nettoyage JSON robuste."""
        if self.df is None or self.target_table is None:
            raise ValueError("DataFrame ou table cible non défini.")
        
        # Nettoyage pandas
        df_clean = self.df.replace({pd.NaT: None})
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        raw_data = df_clean.to_dict(orient='records')
        
        # Nettoyage récursif garanti
        clean_data = [json_safe(record) for record in raw_data]
        
        logger.info(f"Push vers {self.target_table}: {len(clean_data)} enregistrements")
        
        # Chunking
        chunk_size = 500
        for i in range(0, len(clean_data), chunk_size):
            chunk = clean_data[i:i + chunk_size]
            self.supabase.table(self.target_table).insert(chunk).execute()
        
        return len(clean_data)

class DedgeReservationProcessor(BaseProcessor):
    """Processeur simplifié pour D-EDGE Réservations."""
    
    def apply_transformations(self):
        # Déterminer la table cible
        if "COURS" in self.file_path.upper():
            self.target_table = "D-EDGE RÉSERVATIONS EN COURS"
        else:
            self.target_table = "D-EDGE HISTORIQUE DES RÉSERVATIONS N-1"
        
        logger.info(f"Traitement: {self.target_table}")
        
        # Lecture simple
        self.read_excel()
        
        # Injection hotel_id
        self.inject_hotel_id()
        
        # Normalisation des dates
        date_cols = [col for col in self.df.columns if 'date' in col.lower() or 'arrivée' in col.lower() or 'départ' in col.lower()]
        self.normalize_dates(date_cols)
        
        logger.info(f"Réservations: {len(self.df)} lignes prêtes")

class DedgePlanningProcessor(BaseProcessor):
    """Processeur robuste pour D-EDGE Planning."""
    
    def apply_transformations(self):
        self.target_table = "D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES"
        self.read_excel(header=None)
        
        logger.info(f"Planning: Shape = {self.df.shape}")
        
        # Détection de la ligne de dates
        date_row_idx = None
        for r_idx in range(min(6, len(self.df))):
            try:
                test_val = self.df.iloc[r_idx, 2]
                if pd.isna(test_val):
                    continue
                
                # Parser la date
                parsed_date = None
                if isinstance(test_val, (int, float)) and test_val > 40000:
                    parsed_date = pd.to_datetime(test_val, unit='D', origin='1899-12-30')
                elif isinstance(test_val, (datetime, pd.Timestamp)):
                    parsed_date = test_val
                else:
                    parsed_date = pd.to_datetime(test_val, dayfirst=True, errors='coerce')
                
                # Validation simple: année > 2000
                if parsed_date and not pd.isna(parsed_date) and parsed_date.year > 2000:
                    date_row_idx = r_idx
                    logger.info(f"✓ Date row: {r_idx}, test: {parsed_date.strftime('%Y-%m-%d')}")
                    break
            except Exception as e:
                logger.debug(f"Row {r_idx} test failed: {e}")
        
        if date_row_idx is None:
            date_row_idx = 2
            logger.warning("Fallback: row 2")
        
        # Parser toutes les dates
        date_row = self.df.iloc[date_row_idx]
        dates = []
        for val in date_row[2:]:
            try:
                if pd.isna(val):
                    dates.append(None)
                    continue
                
                if isinstance(val, (int, float)):
                    d = pd.to_datetime(val, unit='D', origin='1899-12-30')
                elif isinstance(val, (datetime, pd.Timestamp)):
                    d = val
                else:
                    d = pd.to_datetime(val, dayfirst=True, errors='coerce')
                
                if d and not pd.isna(d) and d.year > 2000:
                    dates.append(d.strftime('%Y-%m-%d'))
                else:
                    dates.append(None)
            except:
                dates.append(None)
        
        logger.info(f"Planning: {len([d for d in dates if d])} dates valides")
        
        # Unpivot
        data_rows = self.df.iloc[date_row_idx + 2:]
        records = []
        current_room_type = None
        
        for idx, row in data_rows.iterrows():
            room_type = row[0]
            rate_plan = row[1]
            
            if pd.notna(room_type) and str(room_type).strip():
                current_room_type = str(room_type)
            
            price_type = row[2]
            if pd.isna(price_type):
                continue
            
            for i, d in enumerate(dates):
                if d is None:
                    continue
                val = row[i+2]
                if pd.isna(val):
                    continue
                
                records.append({
                    "TYPE DE CHAMBRE": current_room_type,
                    "PLAN TARIFAIRE": str(rate_plan) if pd.notna(rate_plan) else None,
                    "LEFT FOR SALE": str(price_type),
                    "date": d,
                    "TARIF / DISPO": str(val),
                    "hotel_id": self.hotel_id
                })
        
        self.df = pd.DataFrame(records)
        logger.info(f"Planning: {len(records)} enregistrements générés")

class OtaInsightProcessor(BaseProcessor):
    """Processeur robuste pour OTA Insight."""
    
    def __init__(self, file_path, hotel_id, supabase_client, tab_name):
        super().__init__(file_path, hotel_id, supabase_client)
        self.tab_name = tab_name

    def apply_transformations(self):
        # Mapping des tables
        tab_map = {
            "Aperçu": "OTA APERÇU",
            "Tarifs": "OTA TARIFS CONCURRENCE",
            "vs. Hier": "OTA VS HIER",
            "vs. 3 jours": "OTA VS 3 JOURS",
            "vs. 7 jours": "OTA VS 7 JOURS"
        }
        
        # Fuzzy matching
        self.target_table = None
        for key, table in tab_map.items():
            if key.lower() in self.tab_name.lower():
                self.target_table = table
                break
        
        if not self.target_table:
            raise ValueError(f"Onglet non reconnu: {self.tab_name}")
        
        logger.info(f"OTA: {self.tab_name} → {self.target_table}")
        
        # Détection d'en-tête
        df_raw = pd.read_excel(self.file_path, sheet_name=self.tab_name, header=None, nrows=15)
        
        header_row = 0
        for i, row in df_raw.iterrows():
            non_empty = [v for v in row.iloc[:5] if pd.notna(v) and str(v).strip()]
            if len(non_empty) >= 2:
                row_str = " ".join([str(v) for v in row]).upper()
                if any(k in row_str for k in ["DATE", "JOUR", "DEMANDE"]):
                    header_row = i
                    break
        
        logger.info(f"OTA: Header à la ligne {header_row}")
        
        # Re-lecture
        self.df = pd.read_excel(self.file_path, sheet_name=self.tab_name, header=header_row)
        
        # Nettoyage colonnes
        new_cols = []
        cols_to_drop = []
        for i, col in enumerate(self.df.columns):
            if str(col).startswith('Unnamed'):
                cols_to_drop.append(i)
            else:
                new_cols.append(snake_case(str(col)))
        
        if cols_to_drop:
            self.df = self.df.drop(self.df.columns[cols_to_drop], axis=1)
            logger.info(f"OTA: {len(cols_to_drop)} colonnes vides supprimées")
        
        self.df.columns = new_cols
        
        # Injection hotel_id
        self.inject_hotel_id()
        
        # Normalisation dates
        date_cols = [col for col in self.df.columns if 'date' in str(col).lower()]
        self.normalize_dates(date_cols)
        
        # Supprimer lignes sans date
        if 'date' in self.df.columns:
            self.df = self.df.dropna(subset=['date'])
        
        logger.info(f"OTA: {len(self.df)} lignes prêtes")

class SalonsEventsProcessor(BaseProcessor):
    """Processeur simple pour Salons & Événements."""
    
    def apply_transformations(self):
        self.target_table = "DATES SALONS ET ÉVÉNEMENTS"
        self.read_excel()
        self.inject_hotel_id()
        
        for col in self.df.columns:
            if 'date' in str(col).lower():
                self.normalize_dates([col])

class ProcessorFactory:
    """Factory pour créer le bon processeur."""
    
    @staticmethod
    def get_processor(category, file_path, hotel_id, supabase_client, tab_name=None):
        if category == "RAPPORT PLANNING D-EDGE":
            return DedgePlanningProcessor(file_path, hotel_id, supabase_client)
        elif category in ["RAPPORT RÉSERVATIONS EN COURS D-EDGE", "RAPPORT HISTORIQUE DES RÉSERVATIONS"]:
            return DedgeReservationProcessor(file_path, hotel_id, supabase_client)
        elif category == "RAPPORT OTA INSIGHT":
            if not tab_name:
                import openpyxl
                wb = openpyxl.load_workbook(file_path, read_only=True)
                tab_name = wb.sheetnames[0]
            return OtaInsightProcessor(file_path, hotel_id, supabase_client, tab_name)
        elif category == "DATES SALONS ET ÉVÉNEMENTS":
            return SalonsEventsProcessor(file_path, hotel_id, supabase_client)
        else:
            raise ValueError(f"Catégorie inconnue: {category}")
