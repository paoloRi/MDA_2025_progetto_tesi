# file_utils.py
import re
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

class DateExtractor:
    """Classe centralizzata per l'estrazione delle date dal nome del file"""
    
    MESI_ITALIANI = {
        'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
        'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
        'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }
    
    @staticmethod
    def extract_date_from_filename(filename: str) -> str:
        """Estrae la data dal nome del file PDF"""
        try:
            # Pattern per formato "31-10-2025"
            pattern1 = r'(\d{2})-(\d{2})-(\d{4})'
            match1 = re.search(pattern1, filename)
            if match1:
                giorno, mese, anno = match1.groups()
                return f"{anno}-{mese}-{giorno}"

            # Pattern per formato "31 ottobre 2025"
            pattern2 = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
            match2 = re.search(pattern2, filename)
            if match2:
                giorno, mese_str, anno = match2.groups()
                mesi = {
                    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
                    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
                    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
                }
                mese_num = mesi.get(mese_str.lower(), '01')
                return f"{anno}-{mese_num}-{giorno.zfill(2)}"

            # Pattern per formato "31.10.2025"
            pattern3 = r'(\d{2})\.(\d{2})\.(\d{4})'
            match3 = re.search(pattern3, filename)
            if match3:
                giorno, mese, anno = match3.groups()
                return f"{anno}-{mese}-{giorno}"

            # Pattern per file 2017-2018 (es: "report_01012017.pdf")
            pattern4 = r'(\d{2})(\d{2})(\d{4})'
            match4 = re.search(pattern4, filename)
            if match4:
                giorno, mese, anno = match4.groups()
                if int(anno) in [2017, 2018]:
                    return f"{anno}-{mese}-{giorno}"

            # Pattern per formato "cruscotto_statistico_giornaliero_31_marzo_2017_2.pdf"
            pattern5 = r'cruscotto_statistico_giornaliero_(\d{1,2})_(\w+)_(\d{4})'
            match5 = re.search(pattern5, filename)
            if match5:
                giorno, mese_str, anno = match5.groups()
                mesi = {
                    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
                    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
                    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
                }
                mese_num = mesi.get(mese_str.lower(), '01')
                return f"{anno}-{mese_num}-{giorno.zfill(2)}"

            # Pattern per formato "31_marzo_2017" (più generico)
            pattern6 = r'(\d{1,2})_(\w+)_(\d{4})'
            match6 = re.search(pattern6, filename)
            if match6:
                giorno, mese_str, anno = match6.groups()
                mesi = {
                    'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
                    'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
                    'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
                }
                mese_num = mesi.get(mese_str.lower(), '01')
                return f"{anno}-{mese_num}-{giorno.zfill(2)}"

        except Exception as e:
            print(f"Errore nell'estrazione data da {filename}: {e}")

        return "Data_non_riconosciuta"

class DataProcessor:
    """Classe per processare dati comuni"""
    
    @staticmethod
    def sort_and_filter_by_date(df: pd.DataFrame, date_column: str = 'data_riferimento', 
                              start_year: int = 2017) -> pd.DataFrame:
        """Ordina e filtra i dati dalla data specificata"""
        if df.empty:
            return df
        
        df_clean = df.copy()
        df_clean['_temp_datetime'] = pd.to_datetime(df_clean[date_column], errors='coerce')
        df_clean = df_clean[df_clean['_temp_datetime'] >= f'{start_year}-01-01']
        df_clean = df_clean.sort_values('_temp_datetime')
        return df_clean.drop('_temp_datetime', axis=1)

class ParquetManager:
    """Gestisce la conversione e il caricamento dei dati in formato Parquet"""
    
    @staticmethod
    def csv_to_parquet(csv_path: Path, parquet_path: Path, compression: str = 'snappy') -> bool:
        """Converte un file CSV in formato Parquet"""
        try:
            df = pd.read_csv(csv_path)
            df.to_parquet(parquet_path, compression=compression, index=False)
            print(f"Convertito: {csv_path.name} -> {parquet_path.name}")
            return True
        except Exception as e:
            print(f"Errore conversione {csv_path.name}: {e}")
            return False
    
    @staticmethod
    def read_parquet(parquet_path: Path) -> pd.DataFrame:
        """Legge un file Parquet in un DataFrame"""
        try:
            return pd.read_parquet(parquet_path)
        except Exception as e:
            print(f"Errore lettura {parquet_path.name}: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def convert_all_csv_to_parquet(csv_directory: Path, parquet_directory: Path) -> Dict[str, bool]:
        """Converte tutti i file CSV nella directory in formato Parquet"""
        parquet_directory.mkdir(exist_ok=True)
        
        csv_files = list(csv_directory.glob("*.csv"))
        results = {}
        
        for csv_file in csv_files:
            parquet_file = parquet_directory / f"{csv_file.stem}.parquet"
            results[csv_file.name] = ParquetManager.csv_to_parquet(csv_file, parquet_file)
        
        return results
