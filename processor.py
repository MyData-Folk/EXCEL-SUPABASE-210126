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
        self.read_excel(header=None)
        
        logger.info(f"Planning: DataFrame shape = {self.df.shape}")
        
        # --- DETECTION ULTRA-ROBUSTE DE LA LIGNE DE DATE ---
        date_row_idx = None
        
        # Essayer plusieurs lignes candidates
        for r_idx in range(min(6, len(self.df))):
            try:
                test_val = self.df.iloc[r_idx, 2]  # Colonne C
                if pd.isna(test_val):
                    continue
                
                # Tester si c'est une date valide
                parsed_date = None
                if isinstance(test_val, (int, float)):
                    # Excel serial date
                    if test_val > 40000:  # Approximativement après 2009
                        parsed_date = pd.to_datetime(test_val, unit='D', origin='1899-12-30')
                elif isinstance(test_val, (datetime, pd.Timestamp)):
                    parsed_date = test_val
                else:
                    # Essayer de parser comme string
                    parsed_date = pd.to_datetime(test_val, dayfirst=True, errors='coerce')
                
                # Valider que la date est dans une plage raisonnable
                if parsed_date and not pd.isna(parsed_date):
                    year = parsed_date.year
                    # TEMPORAIRE: Accepter toutes les années pour diagnostic
                    if year > 1900:  # Juste éviter les erreurs de parsing évidentes
                        date_row_idx = r_idx
                        logger.info(f"✓ Date row trouvée à l'index {r_idx}, date test: {parsed_date.strftime('%Y-%m-%d')} (année {year})")
                        break
                    else:
                        logger.warning(f"✗ Row {r_idx} a une date invalide: {year}")
            except Exception as e:
                logger.debug(f"Row {r_idx} test failed: {e}")
                continue
        
        # Fallback si aucune ligne valide trouvée
        if date_row_idx is None:
            logger.warning("ATTENTION: Aucune ligne de date valide détectée, utilisation de row 2 par défaut")
            date_row_idx = 2
        
        date_row = self.df.iloc[date_row_idx]
        dates = []
        
        for col_idx, val in enumerate(date_row[2:], start=2):
            try:
                if pd.isna(val):
                    dates.append(None)
                    continue
                
                parsed_date = None
                if isinstance(val, (int, float)):
                    parsed_date = pd.to_datetime(val, unit='D', origin='1899-12-30')
                elif isinstance(val, (datetime, pd.Timestamp)):
                    parsed_date = val
                else:
                    parsed_date = pd.to_datetime(val, dayfirst=True, errors='coerce')
                
                # Validation finale - TEMPORAIRE: accepter toutes les années > 1900
                if parsed_date and not pd.isna(parsed_date):
                    if parsed_date.year > 1900:
                        dates.append(parsed_date.strftime('%Y-%m-%d'))
                    else:
                        logger.warning(f"Date col {col_idx} ignorée (année {parsed_date.year})")
                        dates.append(None)
                else:
                    dates.append(None)
            except:
                dates.append(None)
        
        logger.info(f"Planning: {len([d for d in dates if d])} dates valides détectées")
        
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
        logger.info(f"Planning: {len(records)} enregistrements générés")
        
        if len(records) == 0:
            logger.error(f"ALERTE: 0 enregistrements générés! date_row_idx={date_row_idx}, dates_valides={len([d for d in dates if d])}, data_rows={len(data_rows)}")

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
        
        # Mapping plus intelligent (Fuzzy match)
        self.target_table = None
        for key, table in tab_map.items():
            if key.lower() in self.tab_name.lower() or self.tab_name.lower() in key.lower():
                self.target_table = table
                break
        
        if not self.target_table:
            # Fallback direct par mots clés
            if "APER" in self.tab_name.upper(): self.target_table = "OTA APERÇU"
            elif "TARIF" in self.tab_name.upper(): self.target_table = "OTA TARIFS CONCURRENCE"
            elif "HIER" in self.tab_name.upper() or "1 J" in self.tab_name.upper(): self.target_table = "OTA VS HIER"
            elif "3 J" in self.tab_name.upper(): self.target_table = "OTA VS 3 JOURS"
            elif "7 J" in self.tab_name.upper(): self.target_table = "OTA VS 7 JOURS"
            else: raise ValueError(f"Onglet non supporté ou non reconnu: {self.tab_name}")
            
        # --- LECTURE AVEC RECHERCHE D'EN-TÊTE ---
        # OTA Insight a souvent des lignes vides ou logos en haut
        df_raw = pd.read_excel(self.file_path, sheet_name=self.tab_name, header=None, nrows=15)
        
        header_row = 0
        for i, row in df_raw.iterrows():
            # Une ligne de header valide a souvent au moins 3 colonnes non-vides dans les 5 premières
            non_empty = [v for v in row.iloc[:5] if pd.notna(v) and str(v).strip() != ""]
            if len(non_empty) >= 2:
                # Si on trouve un mot clé commun (Date, Jour, Competitor)
                row_str = " ".join([str(v) for v in row]).upper()
                if any(k in row_str for k in ["DATE", "JOUR", "DEMANDE", "COMPS"]):
                    header_row = i
                    break
        
        logger.info(f"OTA Insight: Header trouvé à la ligne {header_row}")
        
        # Re-lire avec le bon header
        self.df = pd.read_excel(self.file_path, sheet_name=self.tab_name, header=header_row)
        
        # Nettoyage des noms de colonnes
        new_cols = []
        cols_to_drop = []
        for i, col in enumerate(self.df.columns):
            original_name = str(col)
            if original_name.startswith('Unnamed'):
                # Marquer pour suppression au lieu de renommer
                cols_to_drop.append(i)
            else:
                # Normalisation snake_case
                clean_name = snake_case(original_name)
                new_cols.append(clean_name)
        
        # Supprimer les colonnes sans nom
        if cols_to_drop:
            self.df = self.df.drop(self.df.columns[cols_to_drop], axis=1)
            logger.info(f"OTA: {len(cols_to_drop)} colonnes vides supprimées")
        
        self.df.columns = new_cols
        
        self.inject_hotel_id()
        # Normalisation des dates si présentes (colonnes contenant date ou jour)
        date_cols = [col for col in self.df.columns if 'date' in str(col).lower() or 'jour' in str(col).lower()]
        self.normalize_dates(date_cols)
        
        # Supprimer les lignes où 'date' est vide (souvent des lignes de résumé en bas)
        if 'date' in self.df.columns:
            self.df = self.df.dropna(subset=['date'])

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
