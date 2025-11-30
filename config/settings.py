# settings.py
from pathlib import Path
from dataclasses import dataclass
import os

@dataclass
class ProjectConfig:
    # Paths relativi alla root del progetto
    BASE_DIR: Path = Path(__file__).parent.parent 
    PDF_SAVE_PATH: Path = BASE_DIR / "pdf"
    OUTPUT_PATH: Path = BASE_DIR / "output"
    
    # Download settings
    BASE_URL: str = "https://libertaciviliimmigrazione.dlci.interno.gov.it/sites/default/files"
    DOMINIO_BASE: str = "https://libertaciviliimmigrazione.dlci.interno.gov.it"
    DOWNLOAD_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    
    # Extraction settings
    DEFAULT_START_YEAR: int = 2017
    DEFAULT_START_MONTH: int = 1
    
    def setup_directories(self):
        """Crea le directory necessarie"""
        self.PDF_SAVE_PATH.mkdir(parents=True, exist_ok=True)
        self.OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Configurazione globale
config = ProjectConfig()
config.setup_directories()
