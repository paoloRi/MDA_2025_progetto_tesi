# pdf_downloader.py
import requests
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
import time

from config.settings import config

class PDFDownloader:
    def __init__(self):
        self.base_url = config.BASE_URL
        self.dominio_base = config.DOMINIO_BASE
        self.save_path = config.PDF_SAVE_PATH
        self.timeout = config.DOWNLOAD_TIMEOUT
        self.max_retries = config.MAX_RETRIES
        
        # Mesi con 31 e 30 giorni
        self.mesi_31_giorni = [1, 3, 5, 7, 8, 10, 12]
        self.mesi_30_giorni = [4, 6, 9, 11]
        
        # Mappatura completa degli URL speciali
        self.url_speciali = self._costruisci_url_speciali()
        
        # Crea directory se non esistono
        self.save_path.mkdir(parents=True, exist_ok=True)

    def _costruisci_url_speciali(self) -> Dict[Tuple[int, int], str]:
        """Costruisce il dizionario completo con gli URL speciali mappati manualmente"""
        return {
            (2017, 1): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_gennaio_2017_3.pdf",
            (2017, 2): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_28_febbraio_2017_2.pdf",
            (2017, 3): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_marzo_2017_2.pdf",
            (2017, 4): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_aprile_2017_3.pdf",
            (2017, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_del_31_maggio_2017_1.pdf",
            (2017, 6): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_del_30_giugno_2017_1.pdf",
            (2017, 7): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_del_31_luglio_2017_1.pdf",
            (2017, 8): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_agosto_2017_1.pdf",
            (2017, 9): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_settembre_1.pdf",
            (2017, 10): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_ottobre_2017_0.pdf",
            (2017, 11): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_novembre_2017_0.pdf",
            (2017, 12): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_dicembre_2017_0.pdf",
            (2018, 1): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_gennaio_2018.pdf",
            (2018, 2): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_28_febbraio_2018.pdf",
            (2018, 3): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_marzo_2018.pdf",
            (2018, 4): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_aprile_2018.pdf",
            (2018, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_maggio_2018.pdf",
            (2018, 6): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_giugno_2018.pdf",
            (2018, 7): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_luglio_2018.pdf",
            (2018, 8): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_agosto_2018.pdf",
            (2018, 9): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_settembre_2018.pdf",
            (2018, 10): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_ottobre_2018.pdf",
            (2018, 11): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_novembre_2018.pdf",
            (2018, 12): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_dicembre_2018.pdf",
            (2019, 1): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31-01-2019_0_0.pdf",
            (2019, 2): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_28-02-2019_0_0.pdf",
            (2019, 3): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31-03-2019_0.pdf",
            (2019, 4): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30-04-2019_0_0.pdf",
            (2019, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31-05-2019_0.pdf",
            (2019, 9): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30-09-2019_0.pdf",
            (2020, 1): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_gennaio_2020.pdf",
            (2020, 3): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_marzo_2020.pdf",
            (2020, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_maggio_2020.pdf",
            (2020, 11): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_novembre_2020.pdf",
            (2020, 12): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_dicembre_2020_0.pdf",
            (2021, 9): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_settembre_2021.pdf",
            (2022, 2): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_28-02-2022_1.pdf",
            (2022, 4): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_aprile_2022.pdf",
            (2022, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31-05-2022%20%281%29.pdf",
            (2022, 11): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_novembre_2022.pdf",
            (2022, 12): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_dicembre_2022.pdf",
            (2024, 3): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31.03.2024.pdf",
            (2024, 4): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_30_aprile_2024.pdf",
            (2024, 6): "/sites/default/files/2025-05/Cruscotto%20statistico%20al%2030%20giugno%202024.pdf",
            (2024, 8): "/sites/default/files/2025-05/Cruscotto%20statistico%20al%2031%20agosto%202024.pdf",
            (2024, 9): "/sites/default/files/2025-05/Cruscotto%20statistico%20al%2030%20settembre%202024.pdf",
            (2024, 10): "/sites/default/files/2025-05/Cruscotto%20statistico%20al%2031%20ottobre%202024.pdf",
            (2024, 11): "/sites/default/files/2025-05/Cruscotto%20statistico%20al%2030%20novembre%202024.pdf",
            (2024, 12): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_31_dicembre_2024.pdf",
            (2025, 5): "/sites/default/files/2025-05/cruscotto_statistico_giornaliero_21-05-2025.pdf",
        }

    def get_ultimo_giorno_mese(self, anno: int, mese: int) -> int:
        """Restituisce l'ultimo giorno del mese"""
        if mese in self.mesi_31_giorni:
            return 31
        elif mese in self.mesi_30_giorni:
            return 30
        else:  # febbraio
            if (anno % 4 == 0 and anno % 100 != 0) or (anno % 400 == 0):
                return 29
            else:
                return 28

    def get_cartella_per_mese(self, anno: int, mese: int) -> str:
        """Determina in quale delle 2 cartelle cercare il file"""
        if anno < 2025 or (anno == 2025 and mese <= 5):
            return "2025-05"
        else:
            return "2025-10"

    def download_pdf(self, url: str, filename: str) -> bool:
        """Scarica un singolo PDF"""
        filepath = self.save_path / filename
        
        # Salta se il file esiste già
        if filepath.exists():
            print(f"File già esistente: {filename}")
            return True
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    file_size = filepath.stat().st_size
                    print(f"Scaricato: {filename} ({file_size} bytes)")
                    return True
                else:
                    print(f"HTTP {response.status_code} per {filename}")
            except Exception as e:
                print(f"Tentativo {attempt + 1} fallito per {filename}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
        
        return False

    def process_mese(self, anno: int, mese: int) -> bool:
        """Processa il download per un mese specifico"""
        print(f"Processando {anno}-{mese:02d}")
        
        # Controlla URL speciale
        if (anno, mese) in self.url_speciali:
            url_relativo = self.url_speciali[(anno, mese)]
            url_completo = self.dominio_base + url_relativo
            nome_file = Path(urllib.parse.unquote(url_relativo)).name
            
            if self.download_pdf(url_completo, nome_file):
                return True
        
        # Tentativo con URL standard
        giorno = self.get_ultimo_giorno_mese(anno, mese)
        cartella = self.get_cartella_per_mese(anno, mese)
        
        # Varianti del nome file
        varianti = [
            f"Cruscotto statistico giornaliero {giorno:02d}-{mese:02d}-{anno}.pdf",
            f"cruscotto_statistico_giornaliero_{giorno:02d}-{mese:02d}-{anno}.pdf",
            f"Cruscotto_statistico_giornaliero_{giorno:02d}-{mese:02d}-{anno}.pdf",
            f"cruscotto_statistico_giornaliero_{giorno}_{mese}_{anno}.pdf",
        ]
        
        for variante in varianti:
            url = f"{self.base_url}/{cartella}/{variante}"
            if self.download_pdf(url, variante):
                return True
        
        print(f"Nessuna variante trovata per {anno}-{mese:02d}")
        return False

    def download_all_pdfs(self, start_year: int = 2017, start_month: int = 1) -> Dict[str, int]:
        """Scarica tutti i PDF dall'inizio a oggi"""
        oggi = datetime.now()
        anno_corrente = oggi.year
        mese_corrente = oggi.month
        
        print(f"Download PDF da {start_month:02d}/{start_year} a {mese_corrente:02d}/{anno_corrente}")
        
        success_count = 0
        total_count = 0
        
        for anno in range(start_year, anno_corrente + 1):
            for mese in range(1, 13):
                # Salta mesi precedenti alla data di inizio
                if anno == start_year and mese < start_month:
                    continue
                # Salta mesi futuri
                if anno == anno_corrente and mese > mese_corrente:
                    break
                
                total_count += 1
                if self.process_mese(anno, mese):
                    success_count += 1
        
        # Report finale
        print(f"\n{'='*50}")
        print(f"RIEPILOGO DOWNLOAD")
        print(f"{'='*50}")
        print(f"Periodi processati: {total_count}")
        print(f"File scaricati: {success_count}")
        print(f"File mancanti: {total_count - success_count}")
        print(f"Percentuale successo: {(success_count/total_count)*100:.1f}%")
        
        return {
            'total': total_count,
            'success': success_count,
            'failed': total_count - success_count
        }

    def get_downloaded_files(self) -> list:
        """Restituisce lista dei file scaricati"""
        pdf_files = list(self.save_path.glob("*.pdf"))
        return sorted([f.name for f in pdf_files])
