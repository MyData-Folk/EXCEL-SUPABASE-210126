import pandas as pd
import os
import json
import logging
from datetime import datetime
from supabase import create_client, Client

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

    def apply_transformations(self):
        """À implémenter par les sous-classes."""
        pass

    def inject_hotel_id(self):
        """Injecte systématiquement la colonne hotel_id."""
        if self.df is not None:
            self.df['hotel_id'] = self.hotel_id

    def normalize_dates(self, date_columns):
        """Normalise les colonnes de date au format YYYY-MM-DD."""
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    def push_to_supabase(self):
        """Pousse les données transformées vers Supabase."""
        if self.df is None or self.target_table is None:
            raise ValueError("DataFrame ou table cible non défini.")
        
        # Conversion du DataFrame en liste de dictionnaires (JSON compatible)
        data = self.df.to_dict(orient='records')
        
        # Chunking pour éviter de dépasser les limites de Supabase
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            result = self.supabase.table(self.target_table).insert(chunk).execute()
        
        return len(data)

class DedgePlanningProcessor(BaseProcessor):
    def apply_transformations(self):
        self.target_table = "D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES"
        # Lecture sans header pour analyser manuellement
        self.read_excel(header=None)
        
        # Identification des dates dans Row 2 ou 3
        # Les dates commencent souvent à la colonne 2
        date_row = self.df.iloc[2]
        dates = []
        for val in date_row[2:]:
            try:
                # Tenter de convertir en date propre
                d = pd.to_datetime(val)
                dates.append(d.strftime('%Y-%m-%d'))
            except:
                dates.append(None)
        
        # Données utiles à partir de Row 4
        data_rows = self.df.iloc[4:]
        
        records = []
        current_room_type = None
        
        for idx, row in data_rows.iterrows():
            room_type = row[0]
            rate_plan = row[1]
            
            # Propagation du type de chambre si vide
            if pd.notna(room_type) and room_type != "":
                current_room_type = room_type
            
            # Le type de prix / dispo
            price_type = row[2] # ex: 'Price (EUR)' or 'Left for sale'
            
            if pd.isna(price_type): continue
            
            # Parcourir les colonnes de dates
            for i, d in enumerate(dates):
                if d is None: continue
                
                val = row[i+2]
                if pd.isna(val): continue
                
                records.append({
                    "TYPE DE CHAMBRE": current_room_type,
                    "PLAN TARIFAIRE": rate_plan if pd.notna(rate_plan) else None,
                    "LEFT FOR SALE": price_type,
                    "date": d,
                    "TARIF / DISPO": str(val),
                    "hotel_id": self.hotel_id
                })
        
        self.df = pd.DataFrame(records)
        logger.info(f"Unpivot terminé: {len(self.df)} lignes générées.")

class DedgeReservationProcessor(BaseProcessor):
    def apply_transformations(self):
        # Cette règle s'applique aux "EN COURS" et "HISTORIQUE"
        self.target_table = "D-EDGE RÉSERVATIONS EN COURS" if "COURS" in self.file_path.upper() else "D-EDGE HISTORIQUE DES RÉSERVATIONS N-1"
        
        # Lecture robuste
        self.read_excel()
        self.inject_hotel_id()
        
        # Normalisation des dates (Recherche de colonnes contenant 'date')
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
            raise ValueError(f"Onglet non supporté: {self.tab_name}")
            
        # Les fichiers OTA Insight ont souvent des lignes d'en-tête à sauter ou des noms sales
        self.read_excel(sheet_name=self.tab_name)
        
        # Nettoyage basique des noms de colonnes (Unnamed -> empty)
        self.df.columns = [col if not str(col).startswith('Unnamed') else f"col_{i}" for i, col in enumerate(self.df.columns)]
        
        self.inject_hotel_id()
        # Normalisation des dates si présentes
        date_cols = [col for col in self.df.columns if 'date' in str(col).lower()]
        self.normalize_dates(date_cols)

class SalonsEventsProcessor(BaseProcessor):
    def apply_transformations(self):
        self.target_table = "DATES SALONS ET ÉVÉNEMENTS"
        self.read_excel()
        self.inject_hotel_id()
        # Normalisation de toutes les colonnes qui ressemblent à des dates
        for col in self.df.columns:
            if self.df[col].dtype == 'object' or 'date' in str(col).lower():
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='ignore')
                    if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                        self.df[col] = self.df[col].dt.strftime('%Y-%m-%d')
                except:
                    pass

class ProcessorFactory:
    @staticmethod
    def get_processor(category, file_path, hotel_id, supabase_client, tab_name=None):
        if category == "RAPPORT PLANNING D-EDGE":
            return DedgePlanningProcessor(file_path, hotel_id, supabase_client)
        elif category in ["RAPPORT RÉSERVATIONS EN COURS D-EDGE", "RAPPORT HISTORIQUE DES RÉSERVATIONS"]:
            return DedgeReservationProcessor(file_path, hotel_id, supabase_client)
        elif category == "RAPPORT OTA INSIGHT":
            return OtaInsightProcessor(file_path, hotel_id, supabase_client, tab_name)
        elif category == "DATES SALONS ET ÉVÉNEMENTS":
            return SalonsEventsProcessor(file_path, hotel_id, supabase_client)
        else:
            raise ValueError(f"Catégorie inconnue: {category}")
