#!/usr/bin/env python3
"""
Pipeline di aggiornamento mensile per dati dei migranti sbarcati e in accoglienza
Eseguito automaticamente via GitHub Actions
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Aggiunge il percorso del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from downloader.pdf_downloader import PDFDownloader
from extractors.nationality_extractor import NationalityExtractor
from extractors.accommodation_extractor import AccommodationExtractor
from extractors.landings_extractor import LandingsExtractor
from utils.file_utils import ParquetManager
from config.settings import config


class MonthlyUpdatePipeline:
    """Gestisce l'aggiornamento mensile dei dati"""
    
    def __init__(self):
        self.current_year = datetime.now().year
        self.current_month = datetime.now().month
        
    def get_previous_month(self) -> tuple:
        """Restituisce mese e anno precedente"""
        today = datetime.now()
        first_of_month = today.replace(day=1)
        previous_month = first_of_month - timedelta(days=1)
        return previous_month.year, previous_month.month
    
    def check_new_data_available(self) -> bool:
        """Verifica se sono disponibili nuovi dati"""
        year, month = self.get_previous_month()
        
        # Controlla se il PDF per il mese precedente esiste già
        expected_patterns = [
            f"*{year}*{month:02d}*",
            f"*{year}*{self._get_month_name(month)}*"
        ]
        
        for pattern in expected_patterns:
            existing_files = list(config.PDF_SAVE_PATH.glob(pattern))
            if existing_files:
                print(f"PDF per {month:02d}/{year} già presente")
                return False
        
        return True
    
    def _get_month_name(self, month: int) -> str:
        """Restituisce il nome del mese in italiano"""
        mesi = {
            1: 'gennaio', 2: 'febbraio', 3: 'marzo', 4: 'aprile',
            5: 'maggio', 6: 'giugno', 7: 'luglio', 8: 'agosto',
            9: 'settembre', 10: 'ottobre', 11: 'novembre', 12: 'dicembre'
        }
        return mesi.get(month, '')
    
    def download_latest_pdf(self) -> bool:
        """Scarica solo l'ultimo PDF disponibile"""
        try:
            downloader = PDFDownloader()
            year, month = self.get_previous_month()
            
            print(f"Scaricando dati per {month:02d}/{year}")
            success = downloader.process_mese(year, month)
            
            return success
            
        except Exception as e:
            print(f"Errore nel download: {e}")
            return False
    
    def update_all_datasets(self):
        """Aggiorna tutti i dataset con i nuovi PDF"""
        pdf_files = list(config.PDF_SAVE_PATH.glob("*.pdf"))
        
        if not pdf_files:
            print("Nessun PDF trovato per l'aggiornamento")
            return
        
        # Processa solo gli ultimi 3 mesi per efficienza
        recent_files = self._get_recent_pdfs(pdf_files, months=3)
        
        print(f"Processando {len(recent_files)} file PDF recenti")
        
        # 1. Aggiorna dati nazionalità
        print("\n--- AGGIORNAMENTO DATI NAZIONALITÀ ---")
        extractor_naz = NationalityExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
        self._update_dataset(extractor_naz, recent_files, "dati_nazionalita.csv")
        
        # 2. Aggiorna dati accoglienza
        print("\n--- AGGIORNAMENTO DATI ACCOGLIENZA ---")
        extractor_acc = AccommodationExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
        self._update_dataset(extractor_acc, recent_files, "dati_accoglienza.csv")
        
        # 3. Aggiorna dati sbarchi giornalieri
        print("\n--- AGGIORNAMENTO DATI SBARCHI GIORNALIERI ---")
        extractor_land = LandingsExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
        self._update_dataset(extractor_land, recent_files, "dati_sbarchi.csv")
    
    def _get_recent_pdfs(self, pdf_files: list, months: int = 3) -> list:
        """Filtra solo i PDF più recenti"""
        cutoff_date = datetime.now() - timedelta(days=months*30)
        recent_files = []
        
        for pdf_file in pdf_files:
            try:
                # Estrae la data dal nome del file
                from utils.file_utils import DateExtractor
                date_str = DateExtractor.extract_date_from_filename(pdf_file.name)
                if date_str != "Data_non_riconosciuta":
                    file_date = datetime.strptime(date_str, '%Y-%m-%d')
                    if file_date >= cutoff_date:
                        recent_files.append(pdf_file)
            except:
                continue
        
        return sorted(recent_files)
    
    def _update_dataset(self, extractor, pdf_files: list, output_filename: str):
        """Aggiorna un singolo dataset"""
        try:
            # Carica i dati esistenti
            existing_data = self._load_existing_data(output_filename)
            
            # Processa i nuovi PDF
            new_data = []
            for pdf_file in pdf_files:
                result = extractor.extract_from_single_pdf(pdf_file)
                if result is not None and not result.empty:
                    new_data.append(result)
            
            if new_data:
                # Combina i dati
                all_new_data = pd.concat(new_data, ignore_index=True)
                
                if not existing_data.empty:
                    # Rimuove duplicati basati su data_riferimento e chiave primaria
                    combined_data = self._merge_datasets(existing_data, all_new_data)
                else:
                    combined_data = all_new_data
                
                # Salva il dataset aggiornato
                combined_data.to_csv(config.OUTPUT_PATH / output_filename, index=False)
                print(f"Aggiornato {output_filename}: {len(combined_data)} righe")
            else:
                print(f"Nessun nuovo dato per {output_filename}")
                
        except Exception as e:
            print(f"Errore nell'aggiornamento di {output_filename}: {e}")
    
    def _load_existing_data(self, filename: str) -> pd.DataFrame:
        """Carica i dati esistenti dal CSV"""
        file_path = config.OUTPUT_PATH / filename
        if file_path.exists():
            return pd.read_csv(file_path)
        return pd.DataFrame()
    
    def _merge_datasets(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """Fonde dataset esistenti e nuovi rimuovendo duplicati"""
        # Determina la colonna chiave in base al tipo di dati
        if 'nazionalita' in existing.columns:
            key_cols = ['data_riferimento', 'nazionalita']
        elif 'regione' in existing.columns:
            key_cols = ['data_riferimento', 'regione']
        elif 'giorno' in existing.columns:
            key_cols = ['data_riferimento', 'giorno']
        else:
            key_cols = ['data_riferimento']
        
        # Rimuove dal dataset esistente i record con date presenti nel nuovo dataset
        existing_dates = new['data_riferimento'].unique()
        existing_clean = existing[~existing['data_riferimento'].isin(existing_dates)]
        
        # Combina i dataset
        return pd.concat([existing_clean, new], ignore_index=True)
    
    def convert_to_parquet(self):
        """Converte tutti i CSV in Parquet"""
        print("\n--- CONVERSIONE IN PARQUET ---")
        parquet_manager = ParquetManager()
        results = parquet_manager.convert_all_csv_to_parquet(config.OUTPUT_PATH, config.OUTPUT_PATH)
        
        success_count = sum(results.values())
        print(f"File convertiti: {success_count}/{len(results)}")
    
    def run_pipeline(self):
        """Esegue l'intera pipeline di aggiornamento"""
        print("=== AVVIO PIPELINE AGGIORNAMENTO DATI MIGRAZIONE ===")
        print(f"Data esecuzione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Verifica disponibilità nuovi dati
            if not self.check_new_data_available():
                print("Nessun nuovo dato disponibile per l'aggiornamento")
                return
            
            # Download nuovo PDF
            if not self.download_latest_pdf():
                print("Download fallito - interrompe pipeline")
                return
            
            # Aggiorna tutti i dataset
            self.update_all_datasets()
            
            # Converti in Parquet
            self.convert_to_parquet()
            
            print("\n=== PIPELINE COMPLETATA CON SUCCESSO ===")
            
        except Exception as e:
            print(f"\n=== PIPELINE FALLITA: {e} ===")
            sys.exit(1)


if __name__ == "__main__":
    pipeline = MonthlyUpdatePipeline()
    pipeline.run_pipeline()
