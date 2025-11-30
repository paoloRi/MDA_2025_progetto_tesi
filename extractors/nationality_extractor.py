# nationality_extractor.py
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Optional, List
import re
import traceback

from extractors.base_extractor import BaseExtractor
from utils.file_utils import DateExtractor, DataProcessor


class NationalityExtractor(BaseExtractor):
    """Estrattore per i dati delle nazionalità dei migranti sbarcati"""
    
    def __init__(self, pdf_folder: Path, output_folder: Path):
        super().__init__(pdf_folder, output_folder)
    
    def extract_from_single_pdf(self, pdf_path: Path) -> Optional[pd.DataFrame]:
        """Estrae i dati delle nazionalità da un singolo PDF"""
        try:
            print(f"Elaborando: {pdf_path.name}")
            
            # Trova la pagina con la tabella delle nazionalità
            page_num = self._find_table_page(pdf_path)
            if page_num is None:
                print(f"  Tabella nazionalità non trovata in {pdf_path.name}")
                return None

            print(f"  Trovata tabella nazionalità a pagina {page_num}")

            # Estrae i dati dalla pagina
            with pdfplumber.open(pdf_path) as pdf:
                page = pdf.pages[page_num]
                table_data = self._extract_table_data(page)
                
                if table_data is not None and not table_data.empty:
                    processed_data = self._process_table_data(table_data, pdf_path.name)
                    print(f"  Estrazione riuscita: {len(processed_data)} nazionalità")
                    return processed_data
                else:
                    print(f"  Nessun dato estratto da {pdf_path.name}")
                    return None
                    
        except Exception as e:
            print(f"Errore nell'elaborazione di {pdf_path.name}: {e}")
            return None

    def _find_table_page(self, pdf_path: Path) -> Optional[int]:
        """Trova la pagina con la tabella delle nazionalità"""
        title_indicators = [
            'Nazionalità dichiarate al momento dello sbarco',
            'Nazionalità dichiarata al momento dello sbarco',
            'Nazionalità dichiarate'
        ]

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in range(len(pdf.pages)):
                    page = pdf.pages[page_num]
                    text = page.extract_text()
                    
                    if text:
                        text_upper = text.upper()
                        # Cerca con gli indicatori del titolo
                        if any(indicator.upper() in text_upper for indicator in title_indicators):
                            return page_num
                        # Cerca con regex
                        if re.search(r'NAZIONALIT[ÀA].*DICHIARAT[AE].*SBARCO', text_upper, re.IGNORECASE):
                            return page_num
            return None
        except Exception as e:
            print(f"Errore nella ricerca della pagina: {e}")
            return None

    def _extract_table_data(self, page) -> Optional[pd.DataFrame]:
        """Estrae i dati della tabella dalla pagina"""
        try:
            # Estrae le tabelle con impostazioni di base
            tables = page.extract_tables({
                "vertical_strategy": "lines", 
                "horizontal_strategy": "lines"
            })
            
            if not tables:
                return None
            
            # Processa ogni tabella trovata
            for table in tables:
                if table and len(table) >= 3:  # Almeno 3 righe
                    df = self._process_table_structure(table)
                    if not df.empty:
                        return df
            
            return None
            
        except Exception as e:
            print(f"Errore nell'estrazione della tabella: {e}")
            return None

    def _normalize_nationality(self, nationality: str) -> str:
        """Normalizza i nomi delle nazionalità con particolare attenzione a Costa d'Avorio"""
        if not nationality:
            return nationality
            
        # Converte in stringa e pulisce spazi
        nationality = str(nationality).strip()
        
        # Normalizza "Costa d'Avorio" e tutte le varianti
        # Gestisce diversi apostrofi, capitalizzazione inconsistente, spazi extra
        costa_patterns = [
            r"costa\s*d[''´`'']\s*avorio",  # "Costa d'avorio"
            r"costa\s*d[''´`'']\s*Avorio",  # "Costa d'Avorio" 
            r"costa\s*D[''´`'']\s*avorio",  # "Costa D'avorio"
            r"costa\s*D[''´`'']\s*Avorio",  # "Costa D'Avorio"
            r"costa\s*dâ€™\s*avorio",       # "Costa dâ€™avorio" (codifica errata)
            r"costa\s*dâ€™\s*Avorio",       # "Costa dâ€™Avorio" (codifica errata)
            r"costa\s*d''\s*avorio",        # "Costa d''avorio" (doppio apostrofo)
            r"costa\s*d''\s*Avorio",        # "Costa d''Avorio" (doppio apostrofo)
        ]
        
        for pattern in costa_patterns:
            if re.search(pattern, nationality, re.IGNORECASE):
                return "Costa d'Avorio"
        
        # Se non è una variante di Costa d'Avorio, mantieni il valore originale
        return nationality

    def _process_table_structure(self, table_data: List[List[str]]) -> pd.DataFrame:
        """Processa la struttura della tabella per estrarre i dati delle nazionalità"""
        try:
            # Pulisci i dati della tabella
            cleaned_data = []
            for row in table_data:
                if row is None:
                    continue
                    
                clean_row = []
                for cell in row:
                    if cell is None:
                        clean_cell = ""
                    else:
                        # Rimuove spazi multipli e newline
                        clean_cell = re.sub(r'\s+', ' ', str(cell).strip())
                    clean_row.append(clean_cell)
                
                # Aggiungi solo righe non vuote
                if any(clean_row) and not all(cell == "" for cell in clean_row):
                    cleaned_data.append(clean_row)
            
            if not cleaned_data:
                return pd.DataFrame()
            
            # Cerca le righe che contengono i dati delle nazionalità
            nationality_rows = []
            
            for row in cleaned_data:
                # Una riga valida deve avere almeno 2 colonne
                if len(row) < 2:
                    continue
                
                nazionalita = row[0]
                migranti_sbarcati = row[1]
                
                # Salta righe che contengono il titolo o vuote
                if not nazionalita or "Nazionalità dichiarate" in nazionalita:
                    continue
                
                # Salta righe vuote nella prima colonna
                if not nazionalita.strip():
                    continue
                
                # Verifica che la seconda colonna contenga un numero
                if not any(char.isdigit() for char in str(migranti_sbarcati)):
                    continue
                
                # Aggiunge ai dati validi
                nationality_rows.append([nazionalita, migranti_sbarcati])
            
            if not nationality_rows:
                return pd.DataFrame()
            
            # Crea DataFrame
            df = pd.DataFrame(nationality_rows, columns=['nazionalita', 'migranti_sbarcati'])
            
            # Pulisce e converte i numeri
            df['migranti_sbarcati'] = (
                df['migranti_sbarcati']
                .astype(str)
                .str.replace(r'[^\d]', '', regex=True)
            )
            df['migranti_sbarcati'] = pd.to_numeric(df['migranti_sbarcati'], errors='coerce')
            df = df.dropna(subset=['migranti_sbarcati'])
            df['migranti_sbarcati'] = df['migranti_sbarcati'].fillna(0).astype(int)
            
            # Filtra righe non valide - MODIFICATO: include "altre"
            df = df[df['nazionalita'].notna() & (df['nazionalita'] != '')]
            df = df[~df['nazionalita'].str.contains(
                r'TOTALE|Totale|NAZIONALITÀ|NAZIONALITA|Note|NOTE|^\s*$', 
                na=False, case=False
            )]
            
            # Filtra paesi con almeno 1 persona
            df = df[df['migranti_sbarcati'] > 0]
            
            # Normalizza i nomi delle nazionalità
            df['nazionalita'] = df['nazionalita'].apply(self._normalize_nationality)
            
            return df
            
        except Exception as e:
            print(f"Errore nel processing della struttura tabellare: {e}")
            return pd.DataFrame()

    def _process_table_data(self, table_data: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Processa i dati della tabella e crea il DataFrame finale"""
        if table_data.empty:
            return table_data

        # Aggiunge i metadati
        date_str = DateExtractor.extract_date_from_filename(filename)
        table_data['data_riferimento'] = date_str
        table_data['filename'] = filename

        return table_data

    def _accumulate_data(self, new_data: pd.DataFrame, filename: str):
        """Accumula i nuovi dati nel dataset completo"""
        if self.complete_data.empty:
            self.complete_data = new_data
        else:
            self.complete_data = pd.concat([self.complete_data, new_data], ignore_index=True)

    def save_to_csv(self, filename: str):
        """Salva i dati accumulati in CSV ordinato per data"""
        if not self.complete_data.empty:
            # Ordina e filtra i dati
            sorted_data = DataProcessor.sort_and_filter_by_date(
                self.complete_data, 
                date_column='data_riferimento',
                start_year=2017
            )
            
            csv_path = self.output_folder / filename
            sorted_data.to_csv(csv_path, index=False)
            
            print(f"\nDati nazionalità salvati in: {csv_path}")
            print(f"Righe totali: {len(sorted_data)}")
            
            # Statistiche per anno
            if 'data_riferimento' in sorted_data.columns:
                sorted_data['anno'] = sorted_data['data_riferimento'].str[:4]
                stats = sorted_data['anno'].value_counts().sort_index()
                print("\nRighe per anno:")
                for anno, count in stats.items():
                    print(f"  {anno}: {count} righe")
                    
            # Statistiche file processati
            success_count = len(self.processed_files) - len(self.failed_files)
            print(f"\nFile processati: {len(self.processed_files)}")
            print(f"File con successo: {success_count}")
            print(f"File falliti: {len(self.failed_files)}")
            
            if self.failed_files:
                print(f"\nPrimi 10 file falliti:")
                for file in self.failed_files[:10]:
                    print(f"  - {file}")
        else:
            print("Nessun dato da salvare")
