# accommodation_extractor.py
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Optional, List
import re
from datetime import datetime

from extractors.base_extractor import BaseExtractor
from utils.file_utils import DateExtractor, DataProcessor


class AccommodationExtractor(BaseExtractor):
    """Estrattore per i dati dei migranti in accoglienza (pre e post giugno 2019)"""
    # tabella con struttura diversa pre e post giugno 2019
    def __init__(self, pdf_folder: Path, output_folder: Path):
        super().__init__(pdf_folder, output_folder)
    
    def extract_from_single_pdf(self, pdf_path: Path) -> Optional[pd.DataFrame]:
        """Estrae i dati dell'accoglienza da un singolo PDF"""
        try:
            print(f"Processando: {pdf_path.name}")
            
            # Trova la pagina con la tabella dell'accoglienza
            page_num = self._find_table_page(pdf_path)
            if page_num is None:
                print(f"  Tabella accoglienza non trovata in {pdf_path.name}")
                return None

            print(f"  Trovata tabella accoglienza a pagina {page_num}")

            # Determina se è formato pre o post 2019
            is_pre_2019 = self._is_pre_2019_format(pdf_path, page_num)
            print(f"  Formato: {'pre-2019' if is_pre_2019 else 'post-2019'}")

            # Estrae i dati dalla tabella
            with pdfplumber.open(pdf_path) as pdf:
                page = pdf.pages[page_num]
                table_data = self._extract_table_data(page, pdf_path.name, is_pre_2019)
                
                if table_data is not None and not table_data.empty:
                    processed_data = self._process_table_data(table_data, pdf_path.name, is_pre_2019)
                    print(f"  Estrazione riuscita: {len(processed_data)} regioni")
                    return processed_data
                else:
                    print(f"  Nessun dato estratto da {pdf_path.name}")
                    return None
                    
        except Exception as e:
            print(f"Errore nell'elaborazione di {pdf_path.name}: {e}")
            return None

    def _is_pre_2019_format(self, pdf_path: Path, page_num: int) -> bool:
        """Determina se la tabella è in formato pre-2019 (3 colonne)"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                page = pdf.pages[page_num]
                text = page.extract_text()
                
                if text:
                    # Cerca indicatori del formato pre-2019
                    pre_2019_indicators = [
                        "Totale immigrati presenti sul territorio regione",
                        "percentuale di distribuzione",
                        "Percentuale di distribuzione"
                    ]
                    
                    if any(indicator in text for indicator in pre_2019_indicators):
                        return True
                    
                    # Controlla anche la data del file
                    date_str = DateExtractor.extract_date_from_filename(pdf_path.name)
                    if date_str != "Data_non_riconosciuta":
                        try:
                            file_date = datetime.strptime(date_str, '%Y-%m-%d')
                            if file_date < datetime(2019, 6, 1):
                                return True
                            # Se la data è 2025, sicuramente è formato post-2019
                            if file_date.year == 2025:
                                return False
                        except ValueError:
                            pass
                
                return False
        except Exception as e:
            print(f"Errore nel determinare il formato del file {pdf_path.name}: {e}")
            # In caso di errore, assume formato post-2019 per sicurezza
            return False

    def _find_table_page(self, pdf_path: Path) -> Optional[int]:
        """Trova la pagina con la tabella delle presenze in accoglienza"""
        title_indicators = [
            'PRESENZE MIGRANTI IN ACCOGLIENZA',
            'PRESENZA MIGRANTI IN ACCOGLIENZA',
            'PRESENZE IN ACCOGLIENZA',
            'PRESENZA IN ACCOGLIENZA',
            'PRESENZE MIGRANTI',
            'PRESENZA MIGRANTI',
            'MIGRANTI IN ACCOGLIENZA',
            'PRESENZE MIGRANTI ACCOGLIENZA',  # Aggiunto per robustezza
            'PRESENZA MIGRANTI ACCOGLIENZA',  # Aggiunto per robustezza
        ]

        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Prima cerca con gli indicatori esatti
                for page_num in range(len(pdf.pages)):
                    page = pdf.pages[page_num]
                    text = page.extract_text()
                    
                    if text:
                        text_upper = text.upper()
                        # Cerca con gli indicatori del titolo
                        if any(indicator.upper() in text_upper for indicator in title_indicators):
                            return page_num
                        # Cerca con regex
                        if re.search(r'PRESENZ[AE]\s*(MIGRANTI)?\s*IN\s*ACCOGLIENZA', text_upper, re.IGNORECASE):
                            return page_num
                        # Cerca anche tabelle che contengono le colonne tipiche per post-2019
                        if all(keyword in text_upper for keyword in ['REGIONE', 'HOT SPOT', 'ACCOGLIENZA']):
                            return page_num
                        # Per i file pre-2019, cerca indicatori specifici
                        if all(keyword in text_upper for keyword in ['REGIONE', 'TOTALE IMMIGRATI PRESENTI']):
                            return page_num
                        # Cerca pattern per file 2025
                        if re.search(r'PRESENZ[AE].*ACCOGLIENZA.*\d{2}/\d{2}/\d{4}', text_upper):
                            return page_num
            
            # Se non trova con gli indicatori esatti, prova con approccio più generico
            for page_num in range(len(pdf.pages)):
                page = pdf.pages[page_num]
                text = page.extract_text()
                
                if text:
                    # Cerca pattern più generici
                    text_upper = text.upper()
                    if 'ACCOGLIENZA' in text_upper and ('MIGRANTI' in text_upper or 'PRESENZ' in text_upper):
                        # Verifica che ci siano indicatori di tabella
                        if 'REGIONE' in text_upper and any(word in text_upper for word in ['TOTALE', 'HOT', 'CENTRI']):
                            return page_num
            
            print(f"  Nessuna tabella accoglienza trovata in {pdf_path.name}")
            return None
            
        except Exception as e:
            print(f"Errore nella ricerca della pagina: {e}")
            return None

    def _extract_table_data(self, page, filename: str, is_pre_2019: bool) -> Optional[pd.DataFrame]:
        """Estrae i dati dalla tabella"""
        try:
            # Prova diversi metodi di estrazione
            extraction_methods = [
                self._extract_with_table_settings,
                self._extract_with_text_analysis,
            ]
            
            for method in extraction_methods:
                result = method(page, filename, is_pre_2019)
                if result is not None and not result.empty:
                    return result
            
            return None
            
        except Exception as e:
            print(f"Errore nell'estrazione della tabella: {e}")
            return None

    def _extract_with_table_settings(self, page, filename: str, is_pre_2019: bool) -> Optional[pd.DataFrame]:
        """Estrae la tabella con impostazioni ottimizzate"""
        try:
            # Impostazioni per l'estrazione tabelle
            table_settings = {
                "vertical_strategy": "lines", 
                "horizontal_strategy": "text",
                "snap_tolerance": 3,
                "join_tolerance": 3,
                "edge_min_length": 3,
                "min_words_vertical": 1,
                "min_words_horizontal": 1,
            }
            
            tables = page.extract_tables(table_settings)
            
            if not tables:
                return None
            
            # Processa ogni tabella trovata
            for table in tables:
                if table and len(table) >= 3:  # Almeno 3 righe
                    if is_pre_2019:
                        df = self._process_pre_2019_table_structure(table, filename)
                    else:
                        df = self._process_post_2019_table_structure(table, filename)
                    
                    if not df.empty:
                        return df
            
            return None
            
        except Exception as e:
            print(f"Errore nell'estrazione con impostazioni: {e}")
            return None

    def _extract_with_text_analysis(self, page, filename: str, is_pre_2019: bool) -> Optional[pd.DataFrame]:
        """Estrae dati con analisi del testo per PDF difficili"""
        try:
            text = page.extract_text()
            if not text:
                return None
            
            lines = text.split('\n')
            
            # Trova la sezione della tabella
            table_start = -1
            for i, line in enumerate(lines):
                if any(indicator in line.upper() for indicator in ['PRESENZ', 'ACCOGLIENZA', 'REGIONE']):
                    table_start = i
                    break
            
            if table_start == -1:
                return None
            
            # Estrai le righe della tabella
            table_lines = []
            for i in range(table_start, min(table_start + 30, len(lines))):  # Massimo 30 righe dopo l'inizio
                line = lines[i].strip()
                if line and not any(keyword in line.upper() for keyword in ['NOTE', 'FONTE', 'ELABORAZIONE']):
                    table_lines.append(line)
            
            if is_pre_2019:
                df = self._process_pre_2019_text_lines(table_lines, filename)
            else:
                df = self._process_post_2019_text_lines(table_lines, filename)
            
            return df
            
        except Exception as e:
            print(f"Errore nell'estrazione con analisi testo: {e}")
            return None

    def _process_pre_2019_table_structure(self, table_data: List[List[str]], filename: str) -> pd.DataFrame:
        """Processa la struttura della tabella pre-2019 (3 colonne)"""
        try:
            # Lista delle regioni italiane per validazione
            regioni_italiane = {
                'abruzzo', 'basilicata', 'calabria', 'campania', 'emilia-romagna',
                'friuli-venezia giulia', 'lazio', 'liguria', 'lombardia', 'marche',
                'molise', 'piemonte', 'puglia', 'sardegna', 'sicilia', 'toscana',
                'trentino-alto adige', 'umbria', "valle d'aosta", 'veneto',
                'trentino alto adige', 'friuli venezia giulia', 'valle d aosta',
                'emilia romagna', 'trentino-alto adige/südtirol'
            }
            
            # Pulisce i dati della tabella
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
                
                # Aggiunge solo righe non vuote
                if any(clean_row) and not all(cell == "" for cell in clean_row):
                    cleaned_data.append(clean_row)
            
            if not cleaned_data:
                return pd.DataFrame()
            
            # Cerca le righe che contengono i dati delle regioni
            region_rows = []
            
            for row in cleaned_data:
                # Per formato pre-2019, almeno 2 colonne (Regione e Totale)
                if len(row) < 2:
                    continue
                
                regione = row[0].strip()
                if not regione:
                    continue
                
                regione_lower = regione.lower()
                
                # Salta righe che contengono il titolo, totali o vuote
                skip_patterns = [
                    "presenze migranti", "presenza migranti", "totale", 
                    "aggiornamento", "regione", "note", "fonte", "percentuale"
                ]
                
                if any(pattern in regione_lower for pattern in skip_patterns):
                    continue
                
                # Verifica che sia una regione italiana valida
                regione_normalized = regione_lower.replace('-', ' ').replace("'", "").replace("'", "").replace(".", "").replace("/", " ").replace("Ã¼", "u").replace("ü", "u")
                regione_words = regione_normalized.split()
                
                # Controlla se almeno una parola della regione corrisponde
                is_regione = any(word in regioni_italiane for word in regione_words if len(word) > 3)
                is_regione_full = regione_normalized in regioni_italiane
                
                # Lista di keyword per identificare le regioni
                strong_keywords = ['lombardia', 'lazio', 'campania', 'sicilia', 'veneto', 'piemonte', 
                                 'toscana', 'puglia', 'emilia', 'sardegna', 'calabria', 'liguria',
                                 'abruzzo', 'marche', 'umbria', 'molise', 'basilicata', 'trentino',
                                 'alto adige', 'friuli', 'valle', 'aosta']
                
                has_strong_keyword = any(keyword in regione_lower for keyword in strong_keywords)
                
                if is_regione or is_regione_full or has_strong_keyword:
                    # Prende la colonna del totale (seconda colonna)
                    totale = row[1] if len(row) > 1 else "0"
                    
                    # Per formato pre-2019, prende solo il totale
                    # Inserisce uno 0 per le altre colonne
                    valid_row = [
                        regione,           # regione
                        "0",               # migranti_hot_spot
                        "0",               # migranti_centri_accoglienza
                        "0",               # migranti_siproimi_sai
                        totale             # totale_accoglienza
                    ]
                    
                    region_rows.append(valid_row)
            
            if not region_rows:
                print(f"  Nessuna regione trovata in {filename}")
                return pd.DataFrame()
            
            # Crea DataFrame con nuovi nomi colonne
            df = pd.DataFrame(region_rows, columns=[
                'regione', 
                'migranti_hot_spot',
                'migranti_centri_accoglienza',
                'migranti_siproimi_sai',
                'totale_accoglienza'
            ])
            
            # Pulisce e converte i numeri
            numeric_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 
                             'migranti_siproimi_sai', 'totale_accoglienza']
            
            for col in numeric_columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r'[^\d]', '', regex=True)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype(int)
            
            # Normalizza i nomi delle regioni
            df = self._normalize_region_names(df)
            
            print(f"  Trovate {len(df)} regioni (formato pre-2019)")
            return df
            
        except Exception as e:
            print(f"Errore nel processing della struttura tabellare pre-2019: {e}")
            return pd.DataFrame()

    def _process_pre_2019_text_lines(self, lines: List[str], filename: str) -> pd.DataFrame:
        """Processa linee di testo per formato pre-2019"""
        try:
            region_rows = []
            
            for line in lines:
                # Pattern per formato pre-2019: "Regione Totale Percentuale%"
                pattern = r'^([A-Za-z\s\-\']+?)\s+(\d{1,3}(?:\.\d{3})*)\s*(\d+,\d+)?%?\s*$'
                match = re.match(pattern, line.strip())
                
                if match:
                    regione = match.group(1).strip()
                    totale = match.group(2).replace('.', '')
                    
                    # Salta intestazioni e totali
                    if any(word in regione.lower() for word in ['presenz', 'regione', 'totale', 'aggiornamento']):
                        continue
                    
                    # Aggiungi la riga
                    region_rows.append([regione, "0", "0", "0", totale])
            
            if not region_rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(region_rows, columns=[
                'regione', 
                'migranti_hot_spot',
                'migranti_centri_accoglienza',
                'migranti_siproimi_sai',
                'totale_accoglienza'
            ])
            
            # Converti i numeri
            numeric_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 
                             'migranti_siproimi_sai', 'totale_accoglienza']
            
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            df = self._normalize_region_names(df)
            return df
            
        except Exception as e:
            print(f"Errore nel processing testo pre-2019: {e}")
            return pd.DataFrame()

    def _process_post_2019_table_structure(self, table_data: List[List[str]], filename: str) -> pd.DataFrame:
        """Processa la struttura della tabella post-2019 (5 colonne)"""
        try:
            # Lista delle regioni italiane per validazione
            regioni_italiane = {
                'abruzzo', 'basilicata', 'calabria', 'campania', 'emilia-romagna',
                'friuli-venezia giulia', 'lazio', 'liguria', 'lombardia', 'marche',
                'molise', 'piemonte', 'puglia', 'sardegna', 'sicilia', 'toscana',
                'trentino-alto adige', 'umbria', "valle d'aosta", 'veneto',
                'trentino alto adige', 'friuli venezia giulia', 'valle d aosta',
                'emilia romagna', 'trentino-alto adige/südtirol'
            }
            
            # Pulisce i dati della tabella
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
                
                # Aggiunge solo righe non vuote
                if any(clean_row) and not all(cell == "" for cell in clean_row):
                    cleaned_data.append(clean_row)
            
            if not cleaned_data:
                return pd.DataFrame()
            
            # Cerca le righe che contengono i dati delle regioni
            region_rows = []
            
            for row in cleaned_data:
                # Una riga valida deve avere almeno 2 colonne
                if len(row) < 2:
                    continue
                
                regione = row[0].strip()
                if not regione:
                    continue
                
                regione_lower = regione.lower()
                
                # Salta righe che contengono il titolo, totali o vuote
                skip_patterns = [
                    "presenze migranti", "presenza migranti", "totale", 
                    "aggiornamento", "regione", "note", "fonte"
                ]
                
                if any(pattern in regione_lower for pattern in skip_patterns):
                    continue
                
                # Verifica che sia una regione italiana valida
                regione_normalized = regione_lower.replace('-', ' ').replace("'", "").replace("'", "").replace(".", "").replace("/", " ").replace("Ã¼", "u").replace("ü", "u")
                regione_words = regione_normalized.split()
                
                # Controlla se almeno una parola della regione corrisponde
                is_regione = any(word in regioni_italiane for word in regione_words if len(word) > 3)
                is_regione_full = regione_normalized in regioni_italiane
                
                # Lista di keyword per identificare regioni
                strong_keywords = ['lombardia', 'lazio', 'campania', 'sicilia', 'veneto', 'piemonte', 
                                 'toscana', 'puglia', 'emilia', 'sardegna', 'calabria', 'liguria',
                                 'abruzzo', 'marche', 'umbria', 'molise', 'basilicata', 'trentino',
                                 'alto adige', 'friuli', 'valle', 'aosta']
                
                has_strong_keyword = any(keyword in regione_lower for keyword in strong_keywords)
                
                if is_regione or is_regione_full or has_strong_keyword:
                    # Prende tutte le colonne disponibili (almeno 2, massimo 5)
                    valid_row = [regione]  # Prima colonna: regione
                    
                    # Aggiunge le colonne numeriche (da 1 a 4)
                    for j in range(1, min(5, len(row))):
                        cell_value = str(row[j])
                        valid_row.append(cell_value)
                    
                    # Se mancano le colonne, aggiunge 0
                    while len(valid_row) < 5:
                        valid_row.append("0")
                    
                    region_rows.append(valid_row)
            
            if not region_rows:
                print(f"  Nessuna regione trovata in {filename}")
                return pd.DataFrame()
            
            # Crea DataFrame con nuovi nomi colonne
            df = pd.DataFrame(region_rows, columns=[
                'regione', 
                'migranti_hot_spot',
                'migranti_centri_accoglienza',
                'migranti_siproimi_sai',
                'totale_accoglienza'
            ])
            
            # Pulisce e converte i numeri
            numeric_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 
                             'migranti_siproimi_sai', 'totale_accoglienza']
            
            for col in numeric_columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r'[^\d]', '', regex=True)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype(int)
            
            # Normalizza i nomi delle regioni
            df = self._normalize_region_names(df)
            
            print(f"  Trovate {len(df)} regioni (formato post-2019)")
            return df
            
        except Exception as e:
            print(f"Errore nel processing della struttura tabellare post-2019: {e}")
            return pd.DataFrame()

    def _process_post_2019_text_lines(self, lines: List[str], filename: str) -> pd.DataFrame:
        """Processa linee di testo per formato post-2019"""
        try:
            region_rows = []
            
            for line in lines:
                # Pattern per formato post-2019: "Regione HotSpot Centri SAI Totale"
                pattern = r'^([A-Za-z\s\-\']+?)\s+(\d*)\s+(\d{1,3}(?:\.\d{3})*)\s+(\d{1,3}(?:\.\d{3})*)\s+(\d{1,3}(?:\.\d{3})*)\s*$'
                match = re.match(pattern, line.strip())
                
                if match:
                    regione = match.group(1).strip()
                    hot_spot = match.group(2) or "0"
                    centri = match.group(3).replace('.', '')
                    sai = match.group(4).replace('.', '')
                    totale = match.group(5).replace('.', '')
                    
                    # Salta intestazioni
                    if any(word in regione.lower() for word in ['presenz', 'regione', 'hot', 'centri']):
                        continue
                    
                    region_rows.append([regione, hot_spot, centri, sai, totale])
            
            if not region_rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(region_rows, columns=[
                'regione', 
                'migranti_hot_spot',
                'migranti_centri_accoglienza',
                'migranti_siproimi_sai',
                'totale_accoglienza'
            ])
            
            # Converti i numeri
            numeric_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 
                             'migranti_siproimi_sai', 'totale_accoglienza']
            
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            df = self._normalize_region_names(df)
            return df
            
        except Exception as e:
            print(f"Errore nel processing testo post-2019: {e}")
            return pd.DataFrame()

    def _normalize_region_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizza i nomi delle regioni"""
        mapping = {
            'TRENTINO-ALTO ADIGE/SÃ¼DTIROL': 'Trentino-Alto Adige',
            'TRENTINO-ALTO ADIGE/SÜDTIROL': 'Trentino-Alto Adige', 
            'TRENTINO-ALTO ADIGE/SUDTIROL': 'Trentino-Alto Adige',
            'TRENTINO-ALTO ADIGE': 'Trentino-Alto Adige',
            'TRENTINO ALTO ADIGE': 'Trentino-Alto Adige',
            'TRENTINO': 'Trentino-Alto Adige',
            'ALTO ADIGE': 'Trentino-Alto Adige',
            
            'VALLE D\'AOSTA': 'Valle D\'Aosta',
            'VALLE D\'AOSTA/VALLÃ©E D\'AOSTE': 'Valle D\'Aosta',
            'VALLE D\'AOSTA/VALLÉE D\'AOSTE': 'Valle D\'Aosta',
            'VALLE D AOSTA/VALLEE D AOSTE': 'Valle D\'Aosta',
            'VALLE D AOSTA': 'Valle D\'Aosta',
            'VALLE DAOSTA': 'Valle D\'Aosta',
            'VALLE D\'AOSTA': 'Valle D\'Aosta',
            'VALLE D\'AOSTA/VALLEE D\'AOSTE': 'Valle D\'Aosta',
            
            'FRIULI-VENEZIA GIULIA': 'Friuli-Venezia Giulia',
            'FRIULI VENEZIA GIULIA': 'Friuli-Venezia Giulia',
            'FRIULI': 'Friuli-Venezia Giulia',
            
            'EMILIA-ROMAGNA': 'Emilia-Romagna',
            'EMILIA ROMAGNA': 'Emilia-Romagna',
            
            'PUGLIE': 'Puglia',
            'TOSCANE': 'Toscana',
            'LOMBARDIE': 'Lombardia'
        }
        
        # Applica il mapping
        df['regione'] = df['regione'].str.upper().replace(mapping).str.title()
        
        return df

    def _process_table_data(self, table_data: pd.DataFrame, filename: str, is_pre_2019: bool) -> pd.DataFrame:
        """Processa i dati della tabella e crea il DataFrame finale"""
        if table_data.empty:
            return table_data

        # Aggiunge i metadati
        date_str = DateExtractor.extract_date_from_filename(filename)
        table_data['data_riferimento'] = date_str
        table_data['filename'] = filename
        table_data['formato'] = 'pre-2019' if is_pre_2019 else 'post-2019'

        return table_data

    def save_to_csv(self, filename: str):
        """Salva i dati accumulati in CSV ordinato per data"""
        if not self.complete_data.empty:
            # Ordina e filtra i dati dal 2017 in poi
            sorted_data = DataProcessor.sort_and_filter_by_date(
                self.complete_data, 
                date_column='data_riferimento',
                start_year=2017
            )
            
            # Assicurati che i dati includano il 2025
            if 'data_riferimento' in sorted_data.columns:
                # Cerca dati del 2025
                data_2025 = sorted_data[sorted_data['data_riferimento'].str.startswith('2025')]
                print(f"\nDati 2025 trovati: {len(data_2025)} righe")
                
                if len(data_2025) > 0:
                    date_2025 = data_2025['data_riferimento'].unique()
                    print(f"Date 2025 presenti: {', '.join(sorted(date_2025))}")
            
            csv_path = self.output_folder / filename
            sorted_data.to_csv(csv_path, index=False)
            
            print(f"\nDati accoglienza salvati in: {csv_path}")
            print(f"Righe totali: {len(sorted_data)}")
            print(f"File processati: {len(self.processed_files)}")
            print(f"File con successo: {len(self.processed_files) - len(self.failed_files)}")
            print(f"File falliti: {len(self.failed_files)}")
            
            # Statistiche per anno
            if 'data_riferimento' in sorted_data.columns:
                sorted_data['anno'] = sorted_data['data_riferimento'].str[:4]
                stats = sorted_data['anno'].value_counts().sort_index()
                print("\nRighe per anno:")
                for anno, count in stats.items():
                    print(f"  {anno}: {count} righe")
            
            # Statistiche per formato
            if 'formato' in sorted_data.columns:
                formato_stats = sorted_data['formato'].value_counts()
                print("\nRighe per formato:")
                for formato, count in formato_stats.items():
                    print(f"  {formato}: {count} righe")
            
            # Statistiche per regione
            regione_stats = sorted_data['regione'].value_counts()
            print(f"\nRegioni trovate: {len(regione_stats)}")
            print("Tutte le regioni:")
            for regione, count in regione_stats.sort_index().items():
                print(f"  {regione}: {count} righe")
                
            # Cerca regioni mancanti
            tutte_regioni = [
                'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
                'Friuli-Venezia Giulia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
                'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Toscana',
                'Trentino-Alto Adige', 'Umbria', 'Valle D\'Aosta', 'Veneto'
            ]
            
            regioni_mancanti = set(tutte_regioni) - set(regione_stats.index)
            if regioni_mancanti:
                print(f"\nRegioni mancanti: {', '.join(regioni_mancanti)}")
            else:
                print(f"\nTutte le 20 regioni sono presenti!")
            
            if self.failed_files:
                print(f"\nFile falliti:")
                for file in self.failed_files:
                    print(f"  - {file}")
        else:
            print("Nessun dato da salvare")
