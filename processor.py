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
            logger.info(f"Fichier lu avec succès: {len(self.df)} lignes.")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la lecture Excel: {str(e)}")
            raise e

    def inject_hotel_id(self):
        """Injecte la colonne hotel_id."""
        if self.df is not None:
            if 'hotel_id' not in self.df.columns:
                self.df['hotel_id'] = self.hotel_id

    def normalize_dates(self, date_columns):
        """Normalise les colonnes de date au format YYYY-MM-DD."""
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    def push_to_supabase(self):
        """Pousse les données transformées vers Supabase avec nettoyage JSON ultra-robuste."""
        if self.df is None or self.target_table is None:
            raise ValueError("DataFrame ou table cible non défini.")
        
        # 1. Premier passage pandas (rapide)
        df_clean = self.df.replace({pd.NaT: None})
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        raw_data = df_clean.to_dict(orient='records')
        
        # 2. Deuxième passage manuel recursif (garanti)
        clean_data = [json_safe(record) for record in raw_data]
        
        # Chunking pour éviter de dépasser les limites de Supabase
        chunk_size = 500
        for i in range(0, len(clean_data), chunk_size):
            chunk = clean_data[i:i + chunk_size]
            self.supabase.table(self.target_table).insert(chunk).execute()
        
        return len(clean_data)

class DedgePlanningProcessor(BaseProcessor):
    def apply_transformations(self):
        self.target_table = "D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES"
        # Lecture sans header pour analyser manuellement
        self.read_excel(header=None)
        
        # --- DETECTION INTELLIGENTE DE LA LIGNE DE DATE ---
        # On cherche une ligne où les colonnes (index 2+) contiennent des dates valides (vers 2026)
        date_row_idx = 2
        for r_idx in [1, 2, 3, 0]:
            if r_idx >= len(self.df): continue
            test_val = self.df.iloc[r_idx, 2] # Colonne C
            if pd.notna(test_val):
                # Si c'est un nombre Excel pour 2026, il doit être > 45000
                if isinstance(test_val, (int, float)) and test_val > 40000:
                    date_row_idx = r_idx
                    break
                # Si c'est un objet date
                if isinstance(test_val, (datetime, pd.Timestamp)):
                    date_row_idx = r_idx
                    break
        
        logger.info(f"Detección ligne de date à l'index: {date_row_idx}")
        date_row = self.df.iloc[date_row_idx]
        dates = []
        for val in date_row[2:]:
            try:
                if pd.isna(val):
                    dates.append(None)
                    continue
                
                if isinstance(val, (int, float)):
                    # Excel serial dates
                    d = pd.to_datetime(val, unit='D', origin='1899-12-30')
                else:
                    d = pd.to_datetime(val, dayfirst=True, errors='coerce')
                
                if pd.isna(d):
                    dates.append(None)
                else:
                    dates.append(d.strftime('%Y-%m-%d'))
            except:
                dates.append(None)
        
        # Données utiles à partir de Row (date_row_idx + 2)
        data_rows = self.df.iloc[date_row_idx + 2:]
        
        records = []
        current_room_type = None
        
        for idx, row in data_rows.iterrows():
            room_type = row[0]
            rate_plan = row[1]
            
            if pd.notna(room_type) and str(room_type).strip() != "":
                current_room_type = str(room_type)
            
            price_type = row[2]
            if pd.isna(price_type): continue
            
            for i, d in enumerate(dates):
                if d is None: continue
                val = row[i+2]
                if pd.isna(val): continue
                
                records.append({
                    "TYPE DE CHAMBRE": current_room_type,
                    "PLAN TARIFAIRE": str(rate_plan) if pd.notna(rate_plan) else None,
                    "LEFT FOR SALE": str(price_type),
                    "date": d,
                    "TARIF / DISPO": str(val),
                    "hotel_id": self.hotel_id
                })
        
        self.df = pd.DataFrame(records)

class DedgeReservationProcessor(BaseProcessor):
    def apply_transformations(self):
        # Cette règle s'applique aux "EN COURS" et "HISTORIQUE"
        self.target_table = "D-EDGE RÉSERVATIONS EN COURS" if "COURS" in self.file_path.upper() else "D-EDGE HISTORIQUE DES RÉSERVATIONS N-1"
        self.read_excel()
        self.inject_hotel_id()
        date_cols = [col for col in self.df.columns if 'date' in col.lower() or 'creation' in col.lower()]
        self.normalize_dates(date_cols)

class OtaInsightProcessor(BaseProcessor):
    def __init__(self, file_path, hotel_id, supabase_client, tab_name):
        super().__init__(file_path, hotel_id, supabase_client)
        self.tab_name = tab_name

    def apply_transformations(self):
        tab_map = {
            "Aperçu": "OTA APERÇU",
            "Tarifs": "OTA TARIFS CONCURRENCE",
            "vs. Hier": "OTA VS HIER",
            "vs. 3 jours": "OTA VS 3 JOURS",
            "vs. 7 jours": "OTA VS 7 JOURS"
        }
        self.target_table = tab_map.get(self.tab_name)
        if not self.target_table:
            # Fallback auto-détection si tab_name est flou
            if "APER" in self.tab_name.upper(): self.target_table = "OTA APERÇU"
            elif "TARIF" in self.tab_name.upper(): self.target_table = "OTA TARIFS CONCURRENCE"
            elif "HIER" in self.tab_name.upper(): self.target_table = "OTA VS HIER"
            else: raise ValueError(f"Onglet non supporté: {self.tab_name}")
            
        self.read_excel(sheet_name=self.tab_name)
        
        new_cols = []
        for i, col in enumerate(self.df.columns):
            if str(col).startswith('Unnamed'):
                new_cols.append(f"col_{i}")
            else:
                new_cols.append(snake_case(str(col)))
        self.df.columns = new_cols
        
        self.inject_hotel_id()
        date_cols = [col for col in self.df.columns if 'date' in str(col).lower()]
        self.normalize_dates(date_cols)

class SalonsEventsProcessor(BaseProcessor):
    def apply_transformations(self):
        self.target_table = "DATES SALONS ET ÉVÉNEMENTS"
        self.read_excel()
        self.inject_hotel_id()
        for col in self.df.columns:
            if 'date' in str(col).lower():
                self.normalize_dates([col])

class ProcessorFactory:
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
