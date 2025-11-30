# base_extractor.py
import pandas as pd
import os
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pathlib import Path

class BaseExtractor(ABC):
    """Classe base per tutti gli estrattori"""
    
    def __init__(self, pdf_folder: Path, output_folder: Path):
        self.pdf_folder = Path(pdf_folder)
        self.output_folder = Path(output_folder)
        self.complete_data = pd.DataFrame()
        self.processed_files = []
        self.failed_files = []
        
        # Crea directory output
        self.output_folder.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def extract_from_single_pdf(self, pdf_path: Path) -> Optional[pd.DataFrame]:
        """Estrae dati da un singolo PDF - da implementare nelle sottoclassi"""
        pass
    
    def process_all_pdfs(self, max_files: Optional[int] = None) -> pd.DataFrame:
        """Processa tutti i PDF nella cartella"""
        pdf_files = [f for f in self.pdf_folder.glob("*.pdf")]
        pdf_files.sort()
        
        if not pdf_files:
            print("Nessun PDF trovato")
            return pd.DataFrame()
        
        if max_files:
            pdf_files = pdf_files[:max_files]
        
        print(f"Processando {len(pdf_files)} file PDF")
        
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n[{i}/{len(pdf_files)}] {pdf_file.name}")
            
            try:
                result = self.extract_from_single_pdf(pdf_file)
                if result is not None and not result.empty:
                    self._accumulate_data(result, pdf_file.name)
                    print(f"Estrazione riuscita: {len(result)} righe")
                else:
                    self.failed_files.append(pdf_file.name)
                    print("Nessun dato estratto")
                    
            except Exception as e:
                self.failed_files.append(pdf_file.name)
                print(f"Errore durante l'estrazione: {e}")
            
            self.processed_files.append(pdf_file.name)
        
        self._generate_report()
        return self.complete_data
    
    def _accumulate_data(self, new_data: pd.DataFrame, filename: str):
        """Accumula i nuovi dati nel dataset completo"""
        if self.complete_data.empty:
            self.complete_data = new_data
        else:
            self.complete_data = pd.concat([self.complete_data, new_data], ignore_index=True)
    
    def save_to_csv(self, filename: str):
        """Salva i dati in CSV"""
        if not self.complete_data.empty:
            csv_path = self.output_folder / filename
            self.complete_data.to_csv(csv_path, index=False)
            print(f"Dati salvati in: {csv_path}")
        else:
            print("Nessun dato da salvare")
    
    def _generate_report(self):
        """Genera report di riepilogo"""
        success_count = len(self.processed_files) - len(self.failed_files)
        
        print(f"\n{'='*50}")
        print("REPORT RIEPILOGO")
        print(f"{'='*50}")
        print(f"File processati: {len(self.processed_files)}")
        print(f"File con successo: {success_count}")
        print(f"File falliti: {len(self.failed_files)}")
        
        if self.failed_files:
            print(f"\nFile falliti:")
            for file in self.failed_files[:5]:  # Mostra solo primi 5
                print(f"  - {file}")
