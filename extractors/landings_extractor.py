# landings_extractor.py
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import re
import calendar
from datetime import datetime

from extractors.base_extractor import BaseExtractor
from utils.file_utils import DateExtractor, DataProcessor


class LandingsExtractor(BaseExtractor):
    """Estrattore per i dati degli sbarchi giornalieri dal grafico"""
    
    def __init__(self, pdf_folder: Path, output_folder: Path):
        super().__init__(pdf_folder, output_folder)
        self.mesi_completi = {
            'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4, 'maggio': 5, 'giugno': 6,
            'luglio': 7, 'agosto': 8, 'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12
        }
        self.mesi_abbr = {
            1: 'gen', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'mag', 6: 'giu',
            7: 'lug', 8: 'ago', 9: 'set', 10: 'ott', 11: 'nov', 12: 'dic'
        }
    
    def extract_from_single_pdf(self, pdf_path: Path) -> Optional[pd.DataFrame]:
        """Estrae i dati degli sbarchi giornalieri da un singolo PDF"""
        try:
            print(f"Elaborando: {pdf_path.name}")
            
            # Estrae la data di riferimento dal filename
            date_str = DateExtractor.extract_date_from_filename(pdf_path.name)
            if date_str == "Data_non_riconosciuta":
                print(f"  Data non riconosciuta per {pdf_path.name}")
                return None

            reference_date = datetime.strptime(date_str, '%Y-%m-%d')
            year = reference_date.year
            month = reference_date.month

            # Estrae i dati usando la strategia basata sulla struttura visiva
            result = self._extract_using_visual_structure(pdf_path, month, year)
            
            if result and result['data']:
                # Crea il DataFrame con i dati trovati
                data = []
                for giorno, migranti_sbarcati in result['data'].items():
                    data.append({
                        'giorno': giorno,
                        'migranti_sbarcati': migranti_sbarcati
                    })
                
                df = pd.DataFrame(data)
                
                # Aggiunge i metadati
                df['data_riferimento'] = date_str
                df['filename'] = pdf_path.name
                
                print(f"  Estrazione riuscita: {len(df)} giorni con dati su {result['expected_days']} totali")
                
                if result['missing_days']:
                    print(f"  Giorni con 0 sbarchi: {len(result['missing_days'])}")
                
                return df
            else:
                print(f"  Nessun dato estratto da {pdf_path.name}")
                return None
                    
        except Exception as e:
            print(f"Errore nell'elaborazione di {pdf_path.name}: {e}")
            return None

    def _extract_using_visual_structure(self, pdf_path: Path, mese: int, anno: int) -> Optional[Dict]:
        """Estrae i dati basandosi sulla struttura visiva specifica del grafico"""
        try:
            abbr_mese = self.mesi_abbr.get(mese, '')
            _, num_giorni_mese = calendar.monthrange(anno, mese)

            with pdfplumber.open(pdf_path) as pdf:
                for page_num in range(len(pdf.pages)):
                    page = pdf.pages[page_num]
                    full_text = page.extract_text()
                    
                    if not full_text:
                        continue

                    # Verifica se questa è la pagina con il grafico usando i marcatori unici
                    if not self._has_unique_chart_markers(full_text):
                        continue

                    print(f"  Trovato grafico a pagina {page_num + 1}")

                    # Estrae solo l'area tra il titolo e le due righe uniche sotto il grafico
                    chart_area_text = self._extract_chart_area_using_unique_markers(full_text)
                    
                    if not chart_area_text:
                        print("  Impossibile isolare l'area del grafico")
                        continue

                    # Estrae i dati solo dall'area del grafico isolata
                    chart_data = self._extract_data_from_chart_area(chart_area_text, abbr_mese, num_giorni_mese)
                    
                    if chart_data and self._validate_visual_structure(chart_data, num_giorni_mese):
                        print(f"  Trovati {len(chart_data)} giorni validi nella struttura visiva")
                        
                        missing_days = [day for day in range(1, num_giorni_mese + 1) if day not in chart_data]

                        return {
                            'data': chart_data,
                            'missing_days': missing_days,
                            'total_days': len(chart_data),
                            'expected_days': num_giorni_mese
                        }
                    else:
                        print("  Dati non validi")
                
                return None
            
        except Exception as e:
            print(f"Errore nell'estrazione con struttura visiva: {e}")
            return None

    def _has_unique_chart_markers(self, text: str) -> bool:
        """
        Verifica se il testo contiene i marcatori unici del grafico visivo
        """
        # 1. Titolo completo con formato specifico
        title_pattern = r'Migranti sbarcati per giorno al \d{1,2} \w+ \d{4}\* - mese di \w+'
        has_title = re.search(title_pattern, text) is not None
        
        # 2. Le due righe sotto il grafico
        unique_markers = [
            r'\*I dati si riferiscono agli eventi di sbarco rilevati entro le ore 8:00 del giorno di riferimento',
            r'Fonte: Dipartimento della Pubblica sicurezza\. I dati sono suscettibili di successivo consolidamento\.'
        ]
        
        has_unique_markers = all(
            re.search(marker, text) for marker in unique_markers
        )
        
        return has_title and has_unique_markers

    def _extract_chart_area_using_unique_markers(self, full_text: str) -> Optional[str]:
        """
        Isola l'area del grafico usando i marcatori unici:
        - Inizio: dopo il titolo completo
        - Fine: prima della prima riga unica sotto il grafico
        """
        try:
            # Trova il titolo completo
            title_pattern = r'Migranti sbarcati per giorno al \d{1,2} \w+ \d{4}\* - mese di \w+'
            title_match = re.search(title_pattern, full_text)
            if not title_match:
                return None
            
            title_end = title_match.end()
            
            # Trova la prima occorrenza della prima riga unica sotto il grafico
            first_unique_line = r'\*I dati si riferiscono agli eventi di sbarco rilevati entro le ore 8:00 del giorno di riferimento'
            first_line_match = re.search(first_unique_line, full_text[title_end:])
            
            if not first_line_match:
                return None
            
            # Estrae il testo tra il titolo e la prima riga unica
            chart_area = full_text[title_end:title_end + first_line_match.start()].strip()
            
            # Rimuove eventuali altri elementi
            chart_area = self._clean_chart_area(chart_area)
            
            return chart_area
            
        except Exception as e:
            print(f"  Errore nell'estrazione area grafico: {e}")
            return None

    def _clean_chart_area(self, chart_area: str) -> str:
        """Pulisce l'area del grafico rimuovendo eventuali altri elementi"""
        noise_patterns = [
            r'Note:.*',
            r'NOTE:.*',
            r'Tabella.*',
            r'PRESENZE.*',
            r'NAZIONALITÀ.*',
            r'Nazionalità.*',
            r'Totale.*'
        ]
        
        cleaned_lines = []
        for line in chart_area.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Controlla se la linea contiene pattern di rumore
            is_noise = any(re.search(pattern, line, re.IGNORECASE) for pattern in noise_patterns)
            if not is_noise:
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)

    def _extract_data_from_chart_area(self, chart_area: str, abbr_mese: str, num_giorni_mese: int) -> Dict[int, int]:
        """Estrae i dati dall'area pulita del grafico"""
        chart_data = {}
        
        # Pattern principale per i dati del grafico
        pattern = r'(\d{1,2})-'+ abbr_mese + r'\s+(\d{1,6})'
        matches = re.findall(pattern, chart_area, re.IGNORECASE)
        
        for day, value in matches:
            day_int = int(day)
            value_int = int(value)
            
            # Validazione base
            if 1 <= day_int <= num_giorni_mese and 0 <= value_int <= 10000:
                chart_data[day_int] = value_int
        
        # Se non trova abbastanza dati, prova pattern alternativi
        if len(chart_data) < num_giorni_mese * 0.3:  # Meno del 30% dei giorni
            chart_data.update(self._extract_with_alternative_patterns(chart_area, abbr_mese, num_giorni_mese))
        
        return chart_data

    def _extract_with_alternative_patterns(self, chart_area: str, abbr_mese: str, num_giorni_mese: int) -> Dict[int, int]:
        """Pattern alternativi per l'estrazione dati"""
        chart_data = {}
        
        patterns = [
            r'(\d{1,2})\s+'+ abbr_mese + r'\s+(\d{1,6})',  
            r'(\d{1,2})[' + abbr_mese + r']\s*(\d{1,6})',  
            r'(\d{1,2})\s+(\d{1,6})',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, chart_area, re.IGNORECASE)
            for match in matches:
                if len(match) == 2:
                    day_int = int(match[0])
                    value_int = int(match[1])
                    if 1 <= day_int <= num_giorni_mese and 0 <= value_int <= 10000:
                        if day_int not in chart_data:  # Evita sovrascritture
                            chart_data[day_int] = value_int
        
        return chart_data

    def _validate_visual_structure(self, chart_data: Dict[int, int], num_giorni_mese: int) -> bool:
        """Valida che i dati estratti rispettino la struttura visiva del grafico"""
        try:
            if not chart_data:
                return False
            
            days = sorted(chart_data.keys())
            values = list(chart_data.values())
            
            # 1. Controlla la presenza di almeno il 25% dei giorni
            if len(days) < max(5, num_giorni_mese * 0.25):
                print(f"    Troppi pochi giorni: {len(days)}/{num_giorni_mese}")
                return False
            
            # 2. Controlla i valori validi
            if any(v < 0 for v in values):
                return False
            
            if any(v > 10000 for v in values):
                print(f"    Valori troppo alti: {max(values) if values else 0}")
                return False
            
            # 3. Controlla la sequenza di giorni
            if days != sorted(days):
                return False
            
            # 4. Controlla eventuali giorni duplicati
            if len(days) != len(set(days)):
                return False
            
            # 5. Controlla la distribuzione giorni
            day_range = max(days) - min(days) + 1
            if day_range < len(days) * 0.8:  # I giorni dovrebbero coprire la maggior parte del mese
                print(f"    Distribuzione giorni anomala: range {day_range}, giorni {len(days)}")
                return False
            
            return True
            
        except Exception as e:
            print(f"    Errore nella validazione struttura visiva: {e}")
            return False

    def _accumulate_data(self, new_data: pd.DataFrame, filename: str):
        """Accumula i nuovi dati nel dataset completo"""
        if self.complete_data.empty:
            self.complete_data = new_data
        else:
            self.complete_data = pd.concat([self.complete_data, new_data], ignore_index=True)

    def save_to_csv(self, filename: str):
        """Salva i dati accumulati in CSV ordinato per data"""
        if not self.complete_data.empty:
            # Ordina e filtra i dati da settembre 2019 in poi 
            sorted_data = DataProcessor.sort_and_filter_by_date(
                self.complete_data, 
                date_column='data_riferimento',
                start_year=2019
            )
            
            # Filtra ulteriormente per includere solo da settembre 2019
            sorted_data = sorted_data[sorted_data['data_riferimento'] >= '2019-09-01']
            
            csv_path = self.output_folder / filename
            sorted_data.to_csv(csv_path, index=False)
            
            print(f"\nDati sbarchi giornalieri salvati in: {csv_path}")
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
            
            # Statistiche per mese
            if 'data_riferimento' in sorted_data.columns:
                sorted_data['mese'] = sorted_data['data_riferimento'].str[5:7]
                mese_stats = sorted_data['mese'].value_counts().sort_index()
                print("\nRighe per mese:")
                for mese, count in mese_stats.items():
                    print(f"  {mese}: {count} righe")
            
            if self.failed_files:
                print(f"\nFile falliti:")
                for file in self.failed_files:
                    print(f"  - {file}")
        else:
            print("Nessun dato da salvare")
