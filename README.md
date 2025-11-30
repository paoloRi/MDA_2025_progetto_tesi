# MDA_2025_progetto_tesi
Analisi dei migranti sbarcati e dei migranti in accoglienza in Italia dal 2017

# Dashboard Analisi Dati Migrazione Italia

Dashboard interattiva per l'analisi dei dati migratori estratti dai report del Ministero dell'Interno italiano.

## Dataset Disponibili

- **dati_nazionalita**: Migranti sbarcati per nazionalità (2017-2025)
- **dati_accoglienza**: Presenze in accoglienza per regione e tipologia (2017-2025)
- **dati_sbarchi**: Sbarchi giornalieri (2019-2025)

## Deployment

La dashboard è deployata su Streamlit Cloud: [Link alla Dashboard](https://your-app-name.streamlit.app)

## Struttura del Progetto

MDA_2025_progetto_tesi/

├── dashboard/ # Applicazione Streamlit

├── config/ # Configurazione progetto

├── utils/ # Utility functions

├── output/ # File Parquet con i dati

└── requirements.txt # Dipendenze Python


## Descrizione
Sistema completo di estrazione, analisi e visualizzazione dati dei migranti sbarcati e dei migranti in accoglienza in Italia dal 2017

## Dashboard Live
[![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://tuo-app.streamlit.app)

## Architettura
- Estrazione Dati: Google Colab + Python
- Database: Parquet + PyArrow
- Visualizzazione: Streamlit + Plotly
- Hosting: Streamlit Cloud + GitHub

## Struttura
- `/dashboard` - Applicazione Streamlit
- `/output` - Dataset in Parquet e CSV
- `/utils` - Utility per elaborazione dati
- `parquet_database.py` - Gestore database analitico

## Riproducibilità
[Link a Google Drive con codice Colab completo]

## Licenza

Questo progetto è distribuito con licenza MIT:

MIT License

Copyright (c) 2025 Paolo Ricciardelli

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
