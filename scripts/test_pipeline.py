#!/usr/bin/env python3
"""Script di test per la pipeline di aggiornamento"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from update_pipeline import MonthlyUpdatePipeline


def test_pipeline():
    """Testa la pipeline in ambiente locale"""
    print("=== TEST PIPELINE AGGIORNAMENTO ===")
    
    pipeline = MonthlyUpdatePipeline()
    
    # Test componenti individuali
    year, month = pipeline.get_previous_month()
    print(f"Mese precedente: {month:02d}/{year}")
    
    data_available = pipeline.check_new_data_available()
    print(f"Nuovi dati disponibili: {data_available}")
    
    # Test merge datasets
    import pandas as pd
    existing = pd.DataFrame({
        'data_riferimento': ['2025-10-01', '2025-10-02'],
        'nazionalita': ['Italia', 'Francia'],
        'migranti_sbarcati': [100, 150]
    })
    
    new = pd.DataFrame({
        'data_riferimento': ['2025-10-02', '2025-10-03'],
        'nazionalita': ['Francia', 'Spagna'],
        'migranti_sbarcati': [200, 120]
    })
    
    merged = pipeline._merge_datasets(existing, new)
    print(f"Merge test: {len(merged)} righe")
    
    print("=== TEST COMPLETATO ===")


if __name__ == "__main__":
    test_pipeline()
