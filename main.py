# main.py
import sys
import subprocess
import os
from pathlib import Path

# Aggiunge il percorso del progetto a sys.path
project_path = '/content/drive/MyDrive/MDA_2025_progetto_tesi'
sys.path.insert(0, project_path)

# Installa le dipendenze necessarie
def install_dependencies():
    try:
        import pdfplumber
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
        print("pdfplumber installato")
    
    try:
        import pyarrow
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow"])
        print("pyarrow installato")

# Installa le dipendenze all'avvio
install_dependencies()

from downloader.pdf_downloader import PDFDownloader
from extractors.nationality_extractor import NationalityExtractor
from extractors.accommodation_extractor import AccommodationExtractor
from extractors.landings_extractor import LandingsExtractor
from utils.file_utils import ParquetManager
from config.settings import config

def check_pdfs_exist():
    """Controlla se ci sono già PDF nella cartella"""
    pdf_files = list(config.PDF_SAVE_PATH.glob("*.pdf"))
    return len(pdf_files) > 0

def main():
    print("Avvio estrazione dati")
    
    # FASE 1: Download PDF (se non presenti)
    pdf_exist = check_pdfs_exist()
    
    if not pdf_exist:
        print("\n" + "="*50)
        print("FASE 1: DOWNLOAD PDF")
        print("="*50)
        
        downloader = PDFDownloader()
        result = downloader.download_all_pdfs(
            start_year=config.DEFAULT_START_YEAR, 
            start_month=config.DEFAULT_START_MONTH
        )
        
        print(f"\nDOWNLOAD COMPLETATO:")
        print(f"File scaricati: {result['success']}/{result['total']}")
        print(f"Percentuale successo: {(result['success']/result['total'])*100:.1f}%")
    else:
        pdf_files = list(config.PDF_SAVE_PATH.glob("*.pdf"))
        print(f"\nPDF già presenti: {len(pdf_files)} file - download non necessario")
    
    # FASE 2: Estrazione dati nazionalità
    print("\n" + "="*50)
    print("FASE 2: ESTRAZIONE DATI NAZIONALITÀ")
    print("="*50)
    
    extractor_naz = NationalityExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
    extractor_naz.process_all_pdfs() 
    extractor_naz.save_to_csv("dati_nazionalita.csv")
    
    # FASE 3: Estrazione dati accoglienza
    print("\n" + "="*50)
    print("FASE 3: ESTRAZIONE DATI ACCOGLIENZA")
    print("="*50)
    
    extractor_acc = AccommodationExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
    extractor_acc.process_all_pdfs()
    extractor_acc.save_to_csv("dati_accoglienza.csv")
    
    # FASE 4: Estrazione dati migranti sbarcati per giorno
    print("\n" + "="*50)
    print("FASE 4: ESTRAZIONE DATI MIGRANTI SBARCATI PER GIORNO")
    print("="*50)
    
    extractor_land = LandingsExtractor(config.PDF_SAVE_PATH, config.OUTPUT_PATH)
    extractor_land.process_all_pdfs()
    extractor_land.save_to_csv("dati_sbarchi.csv")
    
    # FASE 5: Conversione in formato Parquet
    print("\n" + "="*50)
    print("FASE 5: CONVERSIONE IN PARQUET")
    print("="*50)
    
    parquet_manager = ParquetManager()
    results = parquet_manager.convert_all_csv_to_parquet(config.OUTPUT_PATH, config.OUTPUT_PATH)
    
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"\nCONVERSIONE PARQUET COMPLETATA:")
    print(f"File convertiti: {success_count}/{total_count}")
    
    # Verifica lettura file Parquet
    print("\n" + "="*50)
    print("VERIFICA FILE PARQUET")
    print("="*50)
    
    parquet_files = list(config.OUTPUT_PATH.glob("*.parquet"))
    for parquet_file in parquet_files:
        df = parquet_manager.read_parquet(parquet_file)
        print(f"{parquet_file.name}: {len(df)} righe caricate")
    
    print("\nEstrazione e conversione completata")

if __name__ == "__main__":
    main()
