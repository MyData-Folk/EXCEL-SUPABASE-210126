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
                if result and hasattr(result, 'data') and result.get('data'):
                    success_count += len(chunk)
                    logger.debug(f"Chunk {i//chunk_size + 1} inséré avec succès")
                else:
                    # Si pas de réponse data, on considère l'insertion comme échouée
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
                
                # En cas d'erreur, continuer pour ne pas bloquer tout le processus
                # mais journaliser l'erreur pour analyse ultérieure
        
        # Rapport final
        total_chunks = (len(clean_data) + chunk_size - 1) // chunk_size
        logger.info(f"Insertion terminée: {success_count}/{len(clean_data)} enregistrements réussis")
        
        if failed_chunks:
            logger.error(f"{len(failed_chunks)}/{total_chunks} chunks échoués")
            # Sauvegarder les données échouées dans un fichier pour reprise
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
        self.read_excel(header=None)
        
        logger.info(f"Planning: Shape = {self.df.shape}")
        
        # Détection de la ligne de dates
        date_row_idx = None
        for idx, row in self.df.iterrows():
            if pd.notna(row['Date']).any() and isinstance(row['Date'], str):
                date_row_idx = idx
                break
        
        if date_row_idx is None:
            logger.warning("Aucune ligne de dates trouvée")
            return False
        
        logger.info(f"Ligne de dates trouvée à l'index {date_row_idx}")
        
        # Extraire les noms de colonnes de dates
        date_columns = []
        date_row = self.df.iloc[date_row_idx]
        for col in self.df.columns:
            if pd.notna(date_row[col]) and isinstance(date_row[col], str):
                date_columns.append(col)
        
        logger.info(f"Colonnes de dates détectées: {len(date_columns)}")
        
        # Renommer les colonnes pour supprimer les caractères spéciaux
        self.df.columns = [snake_case(str(col)) for col in self.df.columns]
        
        # Normaliser les colonnes de dates
        original_columns = list(self.df.columns)
        date_columns_normalized = []
        for col in date_columns:
            col_normalized = snake_case(col)
            if col_normalized in original_columns:
                self.df[col_normalized] = pd.to_datetime(self.df[col_normalized], errors='coerce').dt.strftime('%Y-%m-%d')
                date_columns_normalized.append(col_normalized)
        
        logger.info(f"Colonnes de dates normalisées: {len(date_columns_normalized)}")
        logger.info(f"Shape final: {self.df.shape}")
        
        return True

class ProcessorFactory:
    """Factory pour instancier les bons processeurs selon le type de fichier."""
    
    @staticmethod
    def get_processor(table_type, file_path, hotel_id, supabase_client: Client):
        """
        Instancie le processeur approprié selon le type de table.
        
        Args:
            table_type (str): Type de données ('planning' ou 'reservation')
            file_path (str): Chemin vers le fichier Excel
            hotel_id (str): Identifiant de l'hôtel
            supabase_client (Client): Client Supabase initialisé
        
        Returns:
            BaseProcessor: Processeur instancié
        
        Raises:
            ValueError: Si le type de table n'est pas supporté
        """
        table_type_lower = table_type.lower()
        
        if "planning" in table_type_lower:
            logger.info(f"Instanciation de DedgePlanningProcessor pour '{table_type}'")
            return DedgePlanningProcessor(file_path, hotel_id, supabase_client)
        elif "reservation" in table_type_lower:
            logger.info(f"Instanciation de DedgeReservationProcessor pour '{table_type}'")
            return DedgeReservationProcessor(file_path, hotel_id, supabase_client)
        else:
            error_msg = f"Type de table inconnu: {table_type}. Attendu: 'planning' ou 'reservation'"
            logger.error(error_msg)
            raise ValueError(error_msg)
