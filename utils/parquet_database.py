# parquet_database.py
"""
Gestore del database in formato Parquet per l'analisi dei dati estratti dai pdf.
Fornisce un'interfaccia simile a un ORM per accedere e interrogare i dati.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
import logging

# NOTA: L'import di config.settings è stato rimosso da qui per evitare
# errori di dipendenze circolari e valutazione prematura in ambienti di deployment.

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ParquetDatabase:
    """
    Database analitico basato su file Parquet.
    Gestisce il caricamento, l'interrogazione e l'analisi dei dati.
    """
    
    def __init__(self, data_directory: Path = None):
        """
        Inizializza il database. Se non viene fornito un percorso,
        usa il percorso predefinito da config.settings.
        """
        # Risolvi il percorso dei dati in modo sicuro, ritardando l'import di config
        if data_directory is None:
            try:
                from config.settings import config
                self.data_directory = config.OUTPUT_PATH
                logger.info(f"Percorso dati configurato da config.settings: {self.data_directory}")
            except ImportError as e:
                # Fallback per sicurezza: directory 'output' nella root del progetto
                logger.warning(f"Impossibile importare config.settings: {e}. Usando percorso di fallback.")
                self.data_directory = Path(__file__).parent.parent / "output"
        else:
            self.data_directory = data_directory
        
        self._data_cache: Dict[str, pd.DataFrame] = {}
        self._metadata: Dict[str, Dict] = {}
        
        # Inizializzazione automatica
        self._initialize_database()
    
    def _initialize_database(self):
        """Inizializza il database caricando i metadati e le tabelle"""
        logger.info("Inizializzazione database Parquet")
        
        # Verifica esistenza directory
        if not self.data_directory.exists():
            logger.warning(f"Directory dati non trovata: {self.data_directory}")
            # Crea la directory se non esiste (potrebbe essere necessario per nuovi deployment)
            try:
                self.data_directory.mkdir(parents=True, exist_ok=True)
                logger.info(f"Directory creata: {self.data_directory}")
            except Exception as e:
                logger.error(f"Errore nella creazione della directory: {e}")
            return
        
        # Carica i metadati delle tabelle
        self._load_table_metadata()
    
    def _load_table_metadata(self):
        """Carica i metadati di tutte le tabelle Parquet disponibili"""
        parquet_files = list(self.data_directory.glob("*.parquet"))
        
        for parquet_file in parquet_files:
            table_name = parquet_file.stem
            try:
                # Legge solo i metadati senza caricare tutto il file
                metadata = pd.read_parquet(parquet_file, engine='pyarrow').dtypes.to_dict()
                self._metadata[table_name] = {
                    'file_path': parquet_file,
                    'columns': list(metadata.keys()),
                    'dtypes': metadata,
                    'size_mb': parquet_file.stat().st_size / (1024 * 1024),
                    'last_modified': datetime.fromtimestamp(parquet_file.stat().st_mtime)
                }
                logger.info(f"Metadati caricati per: {table_name}")
            except Exception as e:
                logger.error(f"Errore caricamento metadati {table_name}: {e}")
    
    def load_all_tables(self) -> Dict[str, pd.DataFrame]:
        """Carica tutte le tabelle Parquet disponibili in memoria"""
        logger.info("Caricamento di tutte le tabelle in memoria")
        
        for table_name, meta in self._metadata.items():
            if table_name not in self._data_cache:
                try:
                    self._data_cache[table_name] = pd.read_parquet(meta['file_path'])
                    logger.info(f"Tabella caricata: {table_name} ({len(self._data_cache[table_name])} righe)")
                except Exception as e:
                    logger.error(f"Errore caricamento {table_name}: {e}")
        
        return self._data_cache
    
    def get_table(self, table_name: str, force_reload: bool = False) -> pd.DataFrame:
        """Restituisce una tabella specifica, caricandola se necessario"""
        if force_reload or table_name not in self._data_cache:
            if table_name in self._metadata:
                try:
                    self._data_cache[table_name] = pd.read_parquet(self._metadata[table_name]['file_path'])
                except Exception as e:
                    logger.error(f"Errore caricamento {table_name}: {e}")
                    return pd.DataFrame()
            else:
                logger.warning(f"Tabella {table_name} non trovata")
                return pd.DataFrame()
        
        return self._data_cache[table_name]
    
    def get_available_tables(self) -> List[str]:
        """Restituisce la lista delle tabelle disponibili"""
        return list(self._metadata.keys())
    
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Restituisce informazioni dettagliate su una tabella"""
        if table_name in self._metadata:
            info = self._metadata[table_name].copy()
            if table_name in self._data_cache:
                df = self._data_cache[table_name]
                info.update({
                    'row_count': len(df),
                    'date_range': self._get_date_range(df),
                    'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
                })
            return info
        return None
    
    def _get_date_range(self, df: pd.DataFrame) -> Dict[str, str]:
        """Estrae l'intervallo di date dai dati"""
        if 'data_riferimento' in df.columns:
            dates = pd.to_datetime(df['data_riferimento'])
            return {
                'min': dates.min().strftime('%Y-%m-%d'),
                'max': dates.max().strftime('%Y-%m-%d')
            }
        return {'min': 'N/A', 'max': 'N/A'}
    
    def query_data(self, table_name: str, 
                   date_column: str = 'data_riferimento',
                   start_date: Optional[Union[str, datetime]] = None,
                   end_date: Optional[Union[str, datetime]] = None,
                   filters: Optional[Dict[str, any]] = None,
                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Esegue query complesse sui dati con filtri multipli
        
        Args:
            table_name: Nome della tabella
            date_column: Colonna da usare per filtri temporali
            start_date: Data iniziale (inclusiva)
            end_date: Data finale (inclusiva)
            filters: Dict di filtri {colonna: valore}
            columns: Lista di colonne da restituire
        
        Returns:
            DataFrame filtrato
        """
        df = self.get_table(table_name)
        
        if df.empty:
            return df
        
        result = df.copy()
        
        # Filtro temporale
        if date_column in df.columns:
            result[date_column] = pd.to_datetime(result[date_column])
            
            if start_date:
                start_date = pd.to_datetime(start_date)
                result = result[result[date_column] >= start_date]
            
            if end_date:
                end_date = pd.to_datetime(end_date)
                result = result[result[date_column] <= end_date]
        
        # Filtri aggiuntivi
        if filters:
            for column, value in filters.items():
                if column in result.columns:
                    if isinstance(value, (list, tuple)):
                        result = result[result[column].isin(value)]
                    else:
                        result = result[result[column] == value]
        
        # Selezione colonne
        if columns:
            available_columns = [col for col in columns if col in result.columns]
            result = result[available_columns]
        
        return result.reset_index(drop=True)
    
    def get_temporal_coverage(self, table_name: str) -> pd.DataFrame:
        """Restituisce la copertura temporale dei dati per anno/mese"""
        df = self.get_table(table_name)
        
        if df.empty or 'data_riferimento' not in df.columns:
            return pd.DataFrame()
        
        df['data_riferimento'] = pd.to_datetime(df['data_riferimento'])
        df['anno'] = df['data_riferimento'].dt.year
        df['mese'] = df['data_riferimento'].dt.month
        
        # Identifica la colonna numerica principale per la somma
        numeric_columns = df.select_dtypes(include=['number']).columns
        main_numeric_column = None
        
        # Cerca colonne numeriche tipiche
        for col in ['migranti_sbarcati', 'totale_accoglienza', 'migranti_hot_spot', 
                    'migranti_centri_accoglienza', 'migranti_siproimi_sai']:
            if col in numeric_columns:
                main_numeric_column = col
                break
        
        # Se non trova colonne specifiche, usa la prima colonna numerica
        if main_numeric_column is None and len(numeric_columns) > 0:
            main_numeric_column = numeric_columns[0]
        
        # Costruisce l'aggregazione
        agg_dict = {'data_riferimento': 'count'}
        if main_numeric_column:
            agg_dict[main_numeric_column] = 'sum'
        
        coverage = df.groupby(['anno', 'mese']).agg(agg_dict).rename(
            columns={'data_riferimento': 'giorni_con_dati'}
        )
        
        if main_numeric_column:
            coverage = coverage.rename(columns={main_numeric_column: 'totale'})
        
        return coverage.reset_index()
    
    def export_to_csv(self, table_name: str, output_path: Path):
        """Esporta una tabella in formato CSV"""
        df = self.get_table(table_name)
        if not df.empty:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Tabella {table_name} esportata in {output_path}")
    
    def get_database_stats(self) -> Dict:
        """Restituisce statistiche complete del database"""
        stats = {
            'total_tables': len(self._metadata),
            'tables': {},
            'total_size_mb': 0,
            'total_rows': 0
        }
        
        for table_name in self.get_available_tables():
            table_info = self.get_table_info(table_name)
            if table_info:
                stats['tables'][table_name] = table_info
                stats['total_size_mb'] += table_info.get('size_mb', 0)
                stats['total_rows'] += table_info.get('row_count', 0)
        
        return stats

# Istanza globale del database per l'applicazione
database = ParquetDatabase()

# Funzioni di utilità per accesso rapido
def get_table_names() -> List[str]:
    """Restituisce i nomi di tutte le tabelle disponibili"""
    return database.get_available_tables()

def quick_query(table_name: str, **kwargs) -> pd.DataFrame:
    """Query rapida con sintassi semplificata"""
    return database.query_data(table_name, **kwargs)

def get_database_info() -> Dict:
    """Restituisce informazioni sul database"""
    return database.get_database_stats()
