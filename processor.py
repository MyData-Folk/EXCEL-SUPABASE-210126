import pandas as pd
import os
import json
import logging
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
        """Pousse les données vers Supabase avec gestion d'erreurs et transactions robuste."""
        if self.df is None or self.target_table is None:
            raise ValueError("DataFrame ou table cible non défini.")
        
        # Nettoyage pandas
        df_clean = self.df.replace({pd.NaT: None})
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        raw_data = df_clean.to_dict(orient='records')
        
        # Nettoyage récursif garanti
        clean_data = [json_safe(record) for record in raw_data]
        
        logger.info(f"Push vers {self.target_table}: {len(clean_data)} enregistrements")
        
        # Chunking avec gestion d'erreurs
        chunk_size = 500
        failed_chunks = []
        success_count = 0
        
        for i in range(0, len(clean_data), chunk_size):
            chunk = clean_data[i:i + chunk_size]
            
            try:
                result = self.supabase.table(self.target_table).insert(chunk).execute()
                
                # Vérifier si l'insertion a réussi
                if result and hasattr(result, 'data'):
                    success_count += len(chunk)
                    logger.debug(f"Chunk {i//chunk_size + 1} inséré avec succès")
                else:
                    raise Exception("Insertion retournée sans données (possible échec partiel)")
                    
            except Exception as e:
                error_msg = f"Erreur chunk {i//chunk_size + 1}: {str(e)}"
                logger.error(error_msg)
                failed_chunks.append({
                    'chunk_index': i//chunk_size + 1,
                    'start': i,
                    'end': i + chunk_size,
                    'error': str(e)
                })
        
        # Rapport final
        total_chunks = (len(clean_data) + chunk_size - 1) // chunk_size
        logger.info(f"Insertion terminée: {success_count}/{len(clean_data)} enregistrements réussis")
        
        if failed_chunks:
            logger.error(f"{len(failed_chunks)}/{total_chunks} chunks échoués")
            self.save_failed_chunks(failed_chunks)
        else:
            logger.info("Tous les chunks insérés avec succès")
        
        return {
            'total': len(clean_data),
            'success': success_count,
            'failed': len(failed_chunks),
            'failed_chunks': failed_chunks
        }

    def save_failed_chunks(self, failed_chunks):
        """Sauvegarde les chunks échoués pour reprise ultérieure."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"/app/logs/failed_chunks_{timestamp}.json"
        
        try:
            os.makedirs('/app/logs', exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(failed_chunks, f, indent=2)
            logger.info(f"Chunks échoués sauvegardés: {filename}")
        except Exception as e:
            logger.error(f"Erreur sauvegarde chunks échoués: {str(e)}")

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
        
        # Lecture sans header pour analyser manuellement
        self.read_excel(header=None)
        
        # Identification dynamique de la ligne contenant les dates
        # On cherche une ligne qui contient plusieurs dates valides
        date_row_idx = None
        dates = []
        
        for i in range(10): # On cherche dans les 10 premières lignes
            row = self.df.iloc[i]
            potential_dates = []
            for val in row[2:]:
                try:
                    d = pd.to_datetime(val)
                    if not pd.isna(d):
                        potential_dates.append(d.strftime('%Y-%m-%d'))
                    else:
                        potential_dates.append(None)
                except:
                    potential_dates.append(None)
            
            # Si on a trouvé au moins 3 dates valides consécutives
            if len([d for d in potential_dates if d]) >= 3:
                date_row_idx = i
                dates = potential_dates
                break
        
        if date_row_idx is None:
            raise ValueError("Impossible de trouver la ligne des dates dans le rapport D-EDGE Planning.")

        # Données utiles à partir de la ligne suivant la ligne de dates
        data_rows = self.df.iloc[date_row_idx + 1:]
        
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
                
                records.append({
                    'hotel_id': self.hotel_id,
                    'room_type': current_room_type,
                    'rate_plan': rate_plan,
                    'price_type': price_type,
                    'date': d,
                    'value': row[i+2]
                })
        
        # Remplacer le DF par les enregistrements aplatis
        self.df = pd.DataFrame(records)
        
        # Nettoyage final des valeurs numériques
        def safe_float(v):
            try: return float(v)
            except: return None
            
        self.df['value'] = self.df['value'].apply(safe_float)
        
        logger.info(f"Planning: {len(self.df)} lignes prêtes")

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
        
        # Détection d'en-tête (nrows=15 pour voir large)
        try:
            df_raw = pd.read_excel(self.file_path, sheet_name=self.tab_name, header=None, nrows=15)
        except Exception as e:
            logger.error(f"Erreur lecture onglet OTA {self.tab_name}: {e}")
            raise e
        
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
        
        # Normalisation dates (flexible: date, jour, etc.)
        date_keywords = ['date', 'jour', 'arrivée', 'départ']
        date_cols = [col for col in self.df.columns if any(k in str(col).lower() for k in date_keywords)]
        logger.info(f"OTA: Colonnes de dates détectées: {date_cols}")
        
        if date_cols:
            self.normalize_dates(date_cols)
            # On garde les lignes qui ont au moins une date valide dans l'une des colonnes détectées
            self.df = self.df.dropna(subset=[date_cols[0]])
        
        logger.info(f"OTA: {len(self.df)} lignes prêtes")

class SalonsEventsProcessor(BaseProcessor):
    """Processeur spécialisé pour les dates des salons et événements."""
    
    def apply_transformations(self):
        self.target_table = "DATES SALONS ET ÉVÉNEMENTS"
        self.read_excel()
        self.inject_hotel_id()
        
        # Normalisation agressive des dates
        for col in self.df.columns:
            if self.df[col].dtype == 'object' or 'date' in str(col).lower():
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                        self.df[col] = self.df[col].dt.strftime('%Y-%m-%d')
                except:
                    pass
        
        logger.info(f"Salons/Événements: {len(self.df)} lignes prêtes")

class ProcessorFactory:
    """Factory pour instancier les bons processeurs selon le type de fichier."""
    
    @staticmethod
    def get_processor(table_type, file_path, hotel_id, supabase_client: Client, tab_name=None):
        table_type_upper = table_type.upper()
        
        if "PLANNING" in table_type_upper:
            return DedgePlanningProcessor(file_path, hotel_id, supabase_client)
        elif "RÉSERVATION" in table_type_upper or "RESERVATION" in table_type_upper:
            return DedgeReservationProcessor(file_path, hotel_id, supabase_client)
        elif "SALON" in table_type_upper or "ÉVÉNEMENT" in table_type_upper or "EVENEMENT" in table_type_upper:
            return SalonsEventsProcessor(file_path, hotel_id, supabase_client)
        elif "OTA" in table_type_upper:
            return OtaInsightProcessor(file_path, hotel_id, supabase_client, tab_name)
        else:
            raise ValueError(f"Type de table inconnu: {table_type}")
