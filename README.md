## MDA_2025_progetto_tesi
Estrazione e analisi dei dati dei migranti sbarcati e in accoglienza in Italia dal 2017

## Dataset Disponibili

- **dati_nazionalita**: numero di migranti sbarcati per nazionalità dal 2017
- **dati_accoglienza**: presenze in accoglienza per regione da ottobre 2017 e per tipologia dei centri di accoglienza da giugno 2019
- **dati_sbarchi**: numero di migranti sbarcati al giorno da settembre 2019

Tutti i dati sono estratti dal **Cruscotto statistico giornaliero** del Ministero dell'Interno italiano.

https://libertaciviliimmigrazione.dlci.interno.gov.it/documentazione/dati-e-statistiche/cruscotto-statistico-giornaliero 

## Deployment
[![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://tuo-app.streamlit.app)
La dashboard pubblica su Streamlit Cloud: https://mda2025progettotesi-zv3cwghtk5kzxttpj3t3fs.streamlit.app/

## Architettura
- Estrazione Dati: pdfplumber
- Database: parquet + pyArrow
- Visualizzazione: Streamlit + plotly
- Hosting: Streamlit Cloud + GitHub

## Struttura
- `/dashboard` - applicazione Streamlit
- `/downloader` - download dei file pdf
- `/extractors` - estrazione dei dati
- `/output` - dataset in Parquet e CSV
- `/scripts` - aggiornamento della dashboard
- `/utils` - utility per elaborazione dati

## Licenza

**Questo progetto è distribuito con licenza MIT:**

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
