import pandas as pd
import os
import json
import logging
import re
from datetime import datetime
from supabase import create_client, Client
from utils import snake_case, json_safe

logger = logging.getLogger(__name__)


def normalize_metric(value: str) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", "", str(value)).strip().lower()


def parse_iso_date(value):
    if pd.isna(value) or value == "":
        return None
    try:
        return pd.to_datetime(value, dayfirst=True, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_time_value(value):
    if pd.isna(value) or value == "":
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%H:%M:%S")


def parse_numeric_value(value):
    if pd.isna(value) or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    cleaned = text.replace("\xa0", "").replace(" ", "")
    if "," in cleaned and "." in cleaned:
        last_comma = cleaned.rfind(",")
        last_dot = cleaned.rfind(".")
        if last_comma > last_dot:
            cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def parse_numeric_zero(value):
    numeric = parse_numeric_value(value)
    return numeric if numeric is not None else 0


def parse_int_value(value):
    numeric = parse_numeric_value(value)
    if numeric is None:
        return None
    try:
        return int(numeric)
    except Exception:
        return None


def clean_text(value):
    if pd.isna(value) or value == "":
        return None
    text = str(value).strip()
    return text if text else None

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

        self.read_excel(header=None)

        date_row_idx = None
        dates = []

        for i in range(10):
            row = self.df.iloc[i]
            potential_dates = []
            for val in row[2:]:
                parsed = pd.to_datetime(val, errors="coerce")
                if not pd.isna(parsed):
                    potential_dates.append(parsed.strftime("%Y-%m-%d"))
                else:
                    potential_dates.append(None)

            if len([d for d in potential_dates if d]) >= 3:
                date_row_idx = i
                dates = potential_dates
                break

        if date_row_idx is None:
            raise ValueError("Impossible de trouver la ligne des dates dans le rapport D-EDGE Planning.")

        data_rows = self.df.iloc[date_row_idx + 1:]

        records = []
        current_room_type = None

        for _, row in data_rows.iterrows():
            room_type = row[0]
            rate_plan = row[1]

            if pd.notna(room_type) and room_type != "":
                current_room_type = room_type

            price_type = row[2]
            if pd.isna(price_type):
                continue

            for i, d in enumerate(dates):
                if d is None:
                    continue
                records.append({
                    "hotel_id": self.hotel_id,
                    "room_type": current_room_type,
                    "rate_plan": rate_plan,
                    "price_type": price_type,
                    "date": d,
                    "value": row[i + 2]
                })

        self.df = pd.DataFrame(records)

        self.df["value"] = self.df["value"].apply(parse_numeric_value)
        logger.info(f"Planning: {len(self.df)} lignes prêtes")

class FolkestonePlanningProcessor(BaseProcessor):
    """Processeur pour le planning Folkestone Opera."""

    def __init__(self, file_path, hotel_id, supabase_client: Client):
        super().__init__(file_path, hotel_id, supabase_client)
        self.tables_data = {}

    def apply_transformations(self):
        self.read_excel(sheet_name="Planning")
        if self.df is None or self.df.empty:
            raise ValueError("Onglet Planning vide ou introuvable.")

        columns = list(self.df.columns)
        if len(columns) < 4:
            raise ValueError("Structure Planning inattendue: colonnes insuffisantes.")

        base_columns = columns[:3]
        date_columns = columns[3:]
        df_long = self.df.melt(
            id_vars=base_columns,
            value_vars=date_columns,
            var_name="date",
            value_name="raw_value"
        )
        df_long = df_long.rename(columns={
            base_columns[0]: "type_de_chambre",
            base_columns[1]: "plan_tarifaire",
            base_columns[2]: "metric"
        })
        df_long["date"] = df_long["date"].apply(parse_iso_date)
        df_long = df_long[df_long["date"].notna()]

        df_long["metric_norm"] = df_long["metric"].apply(normalize_metric)

        dispo_rows = df_long[df_long["metric_norm"].str.contains("leftforsale")]
        dispo_records = []
        for _, row in dispo_rows.iterrows():
            raw_value = row["raw_value"]
            ferme_a_la_vente = None
            disponibilites = None
            if isinstance(raw_value, str) and raw_value.strip().lower() == "x":
                disponibilites = 0
                ferme_a_la_vente = "x"
            else:
                numeric_value = parse_numeric_value(raw_value)
                if numeric_value is not None:
                    disponibilites = numeric_value
                else:
                    disponibilites = None

            dispo_records.append({
                "hotel_id": self.hotel_id,
                "date": row["date"],
                "type_de_chambre": clean_text(row["type_de_chambre"]),
                "disponibilites": disponibilites,
                "ferme_a_la_vente": ferme_a_la_vente
            })

        tarif_rows = df_long[df_long["metric_norm"].apply(lambda v: "price" in v or "tarif" in v)]
        tarif_records = []
        for _, row in tarif_rows.iterrows():
            tarif_records.append({
                "hotel_id": self.hotel_id,
                "date": row["date"],
                "type_de_chambre": clean_text(row["type_de_chambre"]),
                "plan_tarifaire": clean_text(row["plan_tarifaire"]),
                "tarif": parse_numeric_value(row["raw_value"])
            })

        self.tables_data = {
            "disponibilites": dispo_records,
            "planning_tarifs": tarif_records
        }
        self.target_table = list(self.tables_data.keys())

    def push_to_supabase(self):
        total_success = 0
        results = {}
        for table_name, records in self.tables_data.items():
            if not records:
                results[table_name] = 0
                continue
            df = pd.DataFrame(records)
            self.df = df
            self.target_table = table_name
            result = super().push_to_supabase()
            results[table_name] = result
            total_success += result.get("success", 0)
        return results

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

class BookingComProcessor(BaseProcessor):
    """Processeur pour les exports Booking.com multi-onglets."""

    def __init__(self, file_path, hotel_id, supabase_client: Client):
        super().__init__(file_path, hotel_id, supabase_client)
        self.tables_data = {}
        self.date_mise_a_jour = None

    def _load_update_date(self):
        import openpyxl
        wb = openpyxl.load_workbook(self.file_path, data_only=True, read_only=True)
        if "Tarifs" not in wb.sheetnames:
            return None
        sheet = wb["Tarifs"]
        raw_value = sheet["G3"].value
        if raw_value is None:
            return None
        parsed = pd.to_datetime(raw_value, errors="coerce")
        if pd.isna(parsed):
            return None
        if parsed.time().strftime("%H:%M:%S") == "00:00:00":
            return parsed.strftime("%Y-%m-%d")
        return parsed.isoformat()

    def _read_booking_sheet(self, sheet_name, keep_unnamed=False):
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=4)
        df = df.dropna(axis=1, how="all")
        if not keep_unnamed:
            df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
        return df

    def _prepare_infos_table(self, df, hotel_id, date_mise_a_jour):
        df_infos = df.copy()
        for col in df_infos.columns:
            if col in {"hotel_id", "date_mise_a_jour"}:
                continue
            df_infos[col] = df_infos[col].apply(clean_text)
        df_infos.insert(0, "hotel_id", hotel_id)
        df_infos.insert(1, "date_mise_a_jour", date_mise_a_jour)
        return df_infos

    def apply_transformations(self):
        self.date_mise_a_jour = self._load_update_date()
        if not self.date_mise_a_jour:
            raise ValueError("Impossible de lire la date de mise à jour (Tarifs!G3).")

        self.tables_data = {}

        df_apercu = self._read_booking_sheet("Aperçu")
        apercu_numeric_cols = [
            "Votre hôtel le plus bas",
            "Tarif le plus bas",
            "médiane du compset",
            "Demande du marché"
        ]
        if "Date" in df_apercu.columns:
            df_apercu["Date"] = df_apercu["Date"].apply(parse_iso_date)
        for col in apercu_numeric_cols:
            if col in df_apercu.columns:
                df_apercu[col] = df_apercu[col].apply(parse_numeric_zero)
        df_apercu.insert(0, "hotel_id", self.hotel_id)
        df_apercu.insert(1, "date_mise_a_jour", self.date_mise_a_jour)
        self.tables_data["booking_apercu"] = df_apercu

        df_tarifs = self._read_booking_sheet("Tarifs")
        demand_col = next((c for c in df_tarifs.columns if str(c).strip().lower() == "demande du marché"), None)
        if demand_col is None:
            raise ValueError("Colonne 'Demande du marché' introuvable dans Tarifs.")
        demand_idx = list(df_tarifs.columns).index(demand_col)
        competitor_cols = list(df_tarifs.columns)[demand_idx + 1:]
        if "Date" in df_tarifs.columns:
            df_tarifs["Date"] = df_tarifs["Date"].apply(parse_iso_date)
        for col in [demand_col] + competitor_cols:
            df_tarifs[col] = df_tarifs[col].apply(parse_numeric_zero)
        df_tarifs.insert(0, "hotel_id", self.hotel_id)
        df_tarifs.insert(1, "date_mise_a_jour", self.date_mise_a_jour)
        self.tables_data["booking_tarifs"] = df_tarifs

        df_tarifs_raw = self._read_booking_sheet("Tarifs")
        df_tarifs_infos = self._prepare_infos_table(df_tarifs_raw, self.hotel_id, self.date_mise_a_jour)
        self.tables_data["booking_infos_tarifs"] = df_tarifs_infos

        vs_sheets = {
            "vs. Hier": "vs.hier",
            "vs. 3 jours": "vs.3j",
            "vs. 7 jours": "vs.7j"
        }

        for sheet_name, prefix in vs_sheets.items():
            df_vs = self._read_booking_sheet(sheet_name, keep_unnamed=True)
            new_cols = []
            last_named = None
            for col in df_vs.columns:
                col_name = str(col)
                if col_name.startswith("Unnamed") or col_name == "nan":
                    if not last_named:
                        new_cols.append(col_name)
                    else:
                        new_cols.append(f"{prefix}__{last_named}")
                else:
                    new_cols.append(col_name)
                    last_named = col_name
            df_vs.columns = new_cols

            if "Date" in df_vs.columns:
                df_vs["Date"] = df_vs["Date"].apply(parse_iso_date)
            numeric_cols = [c for c in df_vs.columns if c not in {"Jour", "Date"}]
            for col in numeric_cols:
                df_vs[col] = df_vs[col].apply(parse_numeric_zero)
            df_vs.insert(0, "hotel_id", self.hotel_id)
            df_vs.insert(1, "date_mise_a_jour", self.date_mise_a_jour)
            self.tables_data[f"booking_{prefix.replace('.', '_')}"] = df_vs

            df_vs_raw = self._read_booking_sheet(sheet_name, keep_unnamed=True)
            df_vs_raw.columns = new_cols
            df_vs_infos = self._prepare_infos_table(df_vs_raw, self.hotel_id, self.date_mise_a_jour)
            self.tables_data[f"booking_infos_{prefix.replace('.', '_')}"] = df_vs_infos

        self.target_table = list(self.tables_data.keys())

    def push_to_supabase(self):
        results = {}
        for table_name, df in self.tables_data.items():
            df_clean = df.replace({pd.NaT: None}).where(pd.notnull(df), None)
            records = df_clean.to_dict(orient="records")
            clean_records = [json_safe(record) for record in records]
            chunk_size = 500
            success_count = 0
            for i in range(0, len(clean_records), chunk_size):
                chunk = clean_records[i:i + chunk_size]
                self.supabase.table(table_name).insert(chunk).execute()
                success_count += len(chunk)
            results[table_name] = success_count
        return results


class BookingExportProcessor(BaseProcessor):
    """Processeur pour BookingExport."""

    def apply_transformations(self):
        self.target_table = "booking_export"
        self.read_excel()
        if self.df is None:
            raise ValueError("Fichier BookingExport introuvable.")

        date_columns = ["Date d'arrivée", "Date de départ"]
        datetime_columns = {
            "Date d'achat": "Heure d'achat",
            "Dernière modification": "Heure de dernière modification",
            "Date d'annulation": "Heure d'annulation"
        }
        int_columns = [
            "Hôtel (ID)",
            "Nuits",
            "Chambres",
            "Adultes",
            "Enfants",
            "Bébés",
            "Garantie",
            "Partenaire de distribution (ID)"
        ]
        money_columns = ["Montant total", "Montant du panier"]

        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(parse_iso_date)

        for date_col, time_col in datetime_columns.items():
            if date_col in self.df.columns:
                self.df[time_col] = self.df[date_col].apply(parse_time_value)
                self.df[date_col] = self.df[date_col].apply(parse_iso_date)

        for col in int_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(parse_int_value)

        for col in money_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(parse_numeric_value)

        for col in self.df.columns:
            if col in date_columns or col in datetime_columns or col in int_columns or col in money_columns:
                continue
            self.df[col] = self.df[col].apply(clean_text)

        self.inject_hotel_id()
        logger.info(f"BookingExport: {len(self.df)} lignes prêtes")


class EventsCalendarProcessor(BaseProcessor):
    """Processeur pour le calendrier des événements."""

    def apply_transformations(self):
        self.target_table = "events_calendar"
        self.read_excel()
        self.inject_hotel_id()
        if self.df is None:
            raise ValueError("Fichier événements introuvable.")

        date_columns = ["Début", "Fin"]
        numeric_columns = ["Indice impact attendu sur la demande /10", "Multiplicateur"]

        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(parse_iso_date)

        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(parse_numeric_value)

        text_columns = [c for c in self.df.columns if c not in date_columns + numeric_columns + ["hotel_id"]]
        for col in text_columns:
            self.df[col] = self.df[col].apply(clean_text)

        logger.info(f"Events: {len(self.df)} lignes prêtes")

class ProcessorFactory:
    """Factory pour instancier les bons processeurs selon le type de fichier."""
    
    @staticmethod
    def get_processor(table_type, file_path, hotel_id, supabase_client: Client, tab_name=None):
        table_type_upper = (table_type or "").upper()
        
        if "D-EDGE" in table_type_upper and "PLANNING" in table_type_upper:
            return DedgePlanningProcessor(file_path, hotel_id, supabase_client)
        if "FOLKESTONE" in table_type_upper or ("PLANNING" in table_type_upper and "OPERA" in table_type_upper):
            return FolkestonePlanningProcessor(file_path, hotel_id, supabase_client)
        elif "RÉSERVATION" in table_type_upper or "RESERVATION" in table_type_upper:
            return DedgeReservationProcessor(file_path, hotel_id, supabase_client)
        elif "BOOKING.COM" in table_type_upper or ("BOOKING" in table_type_upper and "LOWEST" in table_type_upper):
            return BookingComProcessor(file_path, hotel_id, supabase_client)
        elif "BOOKINGEXPORT" in table_type_upper or "BOOKING EXPORT" in table_type_upper:
            return BookingExportProcessor(file_path, hotel_id, supabase_client)
        elif "SALON" in table_type_upper or "ÉVÉNEMENT" in table_type_upper or "EVENEMENT" in table_type_upper:
            return EventsCalendarProcessor(file_path, hotel_id, supabase_client)
        elif "OTA" in table_type_upper:
            return OtaInsightProcessor(file_path, hotel_id, supabase_client, tab_name)
        else:
            filename_upper = os.path.basename(file_path).upper()
            if "PLANNING" in filename_upper and "FOLKESTONE" in filename_upper:
                return FolkestonePlanningProcessor(file_path, hotel_id, supabase_client)
            if "BOOKINGEXPORT" in filename_upper:
                return BookingExportProcessor(file_path, hotel_id, supabase_client)
            if "BOOKING" in filename_upper and "LOWEST" in filename_upper:
                return BookingComProcessor(file_path, hotel_id, supabase_client)
            if "DATE SALONS" in filename_upper or "EVENEMENTS" in filename_upper:
                return EventsCalendarProcessor(file_path, hotel_id, supabase_client)
            raise ValueError(f"Type de table inconnu: {table_type}")
