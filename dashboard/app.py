# dashboard/app.py
"""
Dashboard Streamlit per l'analisi dei dati migrazione
Integrazione con il sistema Parquet Database esistente
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import sys
from pathlib import Path
import os

# Configurazione pagina Streamlit
st.set_page_config(
    page_title="Analisi del numero dei migranti sbarcati e dei migranti in accoglienza in Italia dal 2017",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurazione sys.path per Streamlit Cloud
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

# Debug nei log di Streamlit
print("=" * 60)
print(f"PROJECT ROOT: {project_root}")
print(f"UTILS PATH: {project_root / 'utils'}")
print(f"UTILS EXISTS: {(project_root / 'utils').exists()}")
print("=" * 60)

# Aggiunge project_root a sys.path se non è già presente
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import dei moduli personalizzati
try:
    # Importa usando il percorso corretto
    from utils.parquet_database import database, get_table_names, quick_query
    IMPORT_SUCCESS = True
    print("Import di parquet_database riuscito")
    
except ImportError as e:
    IMPORT_SUCCESS = False
    # Messaggio di errore dettagliato
    st.error(f"""
    **ERRORE CRITICO NELL'IMPORTAZIONE DEL MODULO 'parquet_database'**

    **Dettaglio tecnico:** {e}

    **Informazioni di sistema Streamlit Cloud:**
    - Directory di esecuzione: `{os.getcwd()}`
    - Project root: `{project_root}`
    - Utils path: `{project_root / 'utils'}`
    - Utils esiste: `{(project_root / 'utils').exists()}`
    - sys.path (primi 5): `{sys.path[:5]}`

    **Cause possibili:**
    1. Il file `utils/__init__.py` non esiste o non è vuoto
    2. Il file `utils/parquet_database.py` contiene errori di sintassi
    3. Il file `config/settings.py` non è accessibile da `parquet_database.py`

    **Azioni richieste:**
    1. Verifica che `utils/__init__.py` esista (deve essere vuoto o contenere solo `__all__ = []`)
    2. Controlla i log completi di Streamlit Cloud per l'errore dettagliato
    3. Verifica che tutti i file siano stati commitati e pushati su GitHub
    """)
    st.stop()

# Cache per le query al database
@st.cache_data(ttl=3600)
def load_table_data(table_name):
    """Carica i dati dalla tabella specificata"""
    return database.get_table(table_name)

# Prende il nome dell'ultimo file scaricato
def get_ultimo_aggiornamento():
    """Restituisce data e filename dell'ultimo aggiornamento"""
    try:
        df = database.get_table('dati_nazionalita')
        if not df.empty and 'data_riferimento' in df.columns:
            ultima_data = df['data_riferimento'].max()
            ultimo_file = df['filename'].iloc[-1] if 'filename' in df.columns else 'N/A'
            return ultima_data, ultimo_file
    except:
        pass
    return "N/A", "N/A"

@st.cache_data(ttl=3600)
def query_filtered_data(table_name, start_date=None, end_date=None, filters=None):
    """Esegue query filtrate sui dati"""
    return database.query_data(
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        filters=filters
    )

# Inizializzazione session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Funzione per ottenere anni e mesi disponibili per dati cumulativi
@st.cache_data(ttl=3600)
def get_available_years_months_for_cumulative():
    """Restituisce gli anni e mesi disponibili per dati cumulativi (nazionalità e accoglienza)"""
    years_months = {}
    
    try:
        # Controlla solo dati_nazionalita e dati_accoglienza
        for table_name in ['dati_nazionalita', 'dati_accoglienza']:
            df = load_table_data(table_name)
            if not df.empty and 'data_riferimento' in df.columns:
                df['data_riferimento'] = pd.to_datetime(df['data_riferimento'])
                df['year'] = df['data_riferimento'].dt.year
                df['month'] = df['data_riferimento'].dt.month
                
                for _, row in df.iterrows():
                    year = int(row['year'])
                    month = int(row['month'])
                    
                    if year not in years_months:
                        years_months[year] = set()
                    years_months[year].add(month)
    except Exception as e:
        st.error(f"Errore nel caricamento degli anni/mesi: {e}")
        return {}
    
    # Ordina gli anni in ordine decrescente (più recenti prima)
    sorted_years_months = {}
    for year in sorted(years_months.keys(), reverse=True):
        sorted_years_months[year] = sorted(years_months[year], reverse=True)
    
    return sorted_years_months

# Funzioni di utilità per l'analisi
def calculate_metrics(df, value_column, period_type='monthly', is_cumulative=False):
    """Calcola metriche principali da un DataFrame con opzioni di periodo"""
    if df.empty:
        return {
            'total': 0
        }
    
    # Per dati cumulativi (singolo mese), il totale è il valore di quel mese
    if is_cumulative and period_type == 'monthly':
        if len(df) > 0:
            metrics = {
                'total': df[value_column].iloc[0] if len(df) == 1 else df[value_column].sum()
            }
        else:
            metrics = {
                'total': 0
            }
    else:
        # Per dati non cumulativi o flussi giornalieri
        metrics = {
            'total': df[value_column].sum()
        }
    
    return metrics

def create_daily_column_chart(df, start_date, end_date):
    """Crea un column chart giornaliero per dati_sbarchi"""
    if df.empty or 'giorno' not in df.columns or 'data_riferimento' not in df.columns:
        return None, None
    
    try:
        # Estrai anno e mese dalla data_riferimento (ultimo giorno del mese)
        df['anno'] = pd.to_datetime(df['data_riferimento']).dt.year
        df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
        
        # Converti giorno in numero
        df['giorno_num'] = pd.to_numeric(df['giorno'], errors='coerce')
        
        # Crea una data completa per ogni giorno
        # Usa l'anno e mese dalla data_riferimento e il giorno dalla colonna 'giorno'
        df['data_completa'] = pd.to_datetime(
            df['anno'].astype(str) + '-' + 
            df['mese'].astype(str) + '-' + 
            df['giorno_num'].astype(str)
        )
        
        # Filtra per il periodo selezionato
        df = df[(df['data_completa'] >= pd.Timestamp(start_date)) & 
                (df['data_completa'] <= pd.Timestamp(end_date))]
        
        if df.empty:
            return None, None
        
        # Crea un dataframe con tutti i giorni nel periodo (anche quelli con 0 sbarchi)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_all_dates = pd.DataFrame({'data_completa': all_dates})
        
        # Unisci con i dati reali
        df_merged = pd.merge(df_all_dates, df[['data_completa', 'migranti_sbarcati']], 
                            on='data_completa', how='left')
        df_merged['migranti_sbarcati'] = df_merged['migranti_sbarcati'].fillna(0)
        
        # Ordina per data
        df_merged = df_merged.sort_values('data_completa')
        
        # Crea il column chart
        fig = px.bar(
            df_merged,
            x='data_completa',
            y='migranti_sbarcati',
            title=f"Sbarchi giornalieri ({start_date} - {end_date})",
            labels={'migranti_sbarcati': 'Migranti sbarcati', 'data_completa': 'Data'},
            color='migranti_sbarcati',
            color_continuous_scale='Viridis'
        )
        
        # Miglioramenti formattazione per tesi
        fig.update_layout(
            font=dict(size=12, family='Arial'),
            plot_bgcolor='white',
            showlegend=False,
            height=500,
            xaxis=dict(
                tickformat='%d %b %Y',
                tickangle=45,
                showgrid=True,
                gridwidth=0.5,
                gridcolor='LightGrey'
            ),
            yaxis=dict(
                showgrid=True,
                gridwidth=0.5,
                gridcolor='LightGrey'
            )
        )
        
        return fig, df_merged
        
    except Exception as e:
        st.error(f"Errore nella creazione del grafico: {str(e)}")
        return None, None

def create_nationality_bar_chart(df, year=None, month=None):
    """Crea un bar chart ordinato per tutte le nazionalità"""
    if df.empty:
        return None
    
    # Calcola i totali e ordina
    nationality_totals = df.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
    nationality_totals = nationality_totals.sort_values('migranti_sbarcati', ascending=False)
    
    # Prepara titolo con mese e anno
    month_names = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names.get(month, "")
    title_suffix = f" - {month_name}-{year}" if month and year else ""
    
    fig = px.bar(
        nationality_totals,
        x='migranti_sbarcati',
        y='nazionalita',
        orientation='h',
        title=f"Distribuzione per nazionalità{title_suffix}",
        labels={'migranti_sbarcati': 'Totale migranti sbarcati', 'nazionalita': 'Nazionalità'},
        color='migranti_sbarcati',
        color_continuous_scale='Viridis_r'
    )
    
    # Miglioramenti per tesi
    fig.update_layout(
        font=dict(size=12),
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='white',
        height=800  # Altezza maggiore per mostrare tutte le nazionalità
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    
    return fig

def create_simple_regional_map(df, year=None, month=None):
    """Crea una mappa semplificata per le regioni italiane"""
    if df.empty or 'regione' not in df.columns:
        return None
    
    # Coordinate delle 20 regioni italiane (lat, lon)
    region_coordinates = {
        'Abruzzo': [42.4, 13.8],
        'Basilicata': [40.5, 16.0],
        'Calabria': [39.0, 16.5],
        'Campania': [40.8, 14.8],
        'Emilia-Romagna': [44.5, 11.0],
        'Friuli-Venezia Giulia': [46.0, 13.0],
        'Lazio': [41.9, 12.5],
        'Liguria': [44.4, 8.9],
        'Lombardia': [45.6, 9.4],
        'Marche': [43.3, 13.0],
        'Molise': [41.7, 14.6],
        'Piemonte': [45.1, 7.7],
        'Puglia': [41.1, 16.9],
        'Sardegna': [40.0, 9.0],
        'Sicilia': [37.5, 14.0],
        'Toscana': [43.8, 11.0],
        'Trentino-Alto Adige': [46.5, 11.3],
        'Umbria': [43.0, 12.5],
        "Valle D'Aosta": [45.7, 7.4],
        'Veneto': [45.4, 11.9]
    }
    
    # Prepara i dati per la mappa
    map_data = []
    for regione, coords in region_coordinates.items():
        region_df = df[df['regione'] == regione]
        if not region_df.empty:
            total = region_df['totale_accoglienza'].sum()
            map_data.append({
                'regione': regione,
                'lat': coords[0],
                'lon': coords[1],
                'totale_accoglienza': total
            })
    
    if not map_data:
        return None
    
    map_df = pd.DataFrame(map_data)
    
    # Prepara titolo con mese e anno
    month_names = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names.get(month, "")
    title_suffix = f" - {month_name}-{year}" if month and year else ""
    
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        size="totale_accoglienza",
        color="totale_accoglienza",
        hover_name="regione",
        hover_data={
            "totale_accoglienza": True,
            "lat": False,
            "lon": False
        },
        title=f"Distribuzione regionale migranti in accoglienza{title_suffix}",
        size_max=30,
        zoom=4.8,
        center={"lat": 42.5, "lon": 12.5},
        color_continuous_scale=px.colors.sequential.Viridis_r,
        height=500
    )
    
    fig.update_layout(mapbox_style="carto-positron")
    
    fig.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        font=dict(size=12),
        coloraxis_colorbar=dict(
            title="Totale",
            thicknessmode="pixels",
            thickness=15,
            lenmode="pixels",
            len=300,
            yanchor="top",
            y=0.95,
            xanchor="left",
            x=0.01
        )
    )
    
    return fig

def create_accommodation_bar_chart(df, selected_types=None, year=None, month=None):
    """Crea un bar chart per le tipologie di accoglienza"""
    if df.empty:
        return None
    
    # Definisci le colonne delle tipologie
    type_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 'migranti_siproimi_sai']
    
    # Se sono specificate le tipologie selezionate, mappale alle colonne
    if selected_types is not None and len(selected_types) > 0:
        type_names_to_col = {
            'Hot Spot': 'migranti_hot_spot',
            'Centri Accoglienza': 'migranti_centri_accoglienza',
            'SIPROIMI/SAI': 'migranti_siproimi_sai'
        }
        # Filtra le colonne in base alle tipologie selezionate
        available_cols = [type_names_to_col[tip] for tip in selected_types if tip in type_names_to_col]
    else:
        # Altrimenti, prendi tutte le colonne disponibili
        available_cols = [col for col in type_columns if col in df.columns]
    
    if not available_cols:
        return None
    
    # Calcola i totali per tipologia
    type_totals = df[available_cols].sum().reset_index()
    type_totals.columns = ['tipologia', 'totale']
    
    # Mappa i nomi più leggibili
    type_names = {
        'migranti_hot_spot': 'Hot Spot',
        'migranti_centri_accoglienza': 'Centri Accoglienza',
        'migranti_siproimi_sai': 'SIPROIMI/SAI'
    }
    
    type_totals['tipologia'] = type_totals['tipologia'].map(type_names)
    
    # Ordina in modo decrescente per 'totale'
    type_totals = type_totals.sort_values('totale', ascending=False)
    
    # Prepara titolo con mese e anno
    month_names_dict = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names_dict.get(month, "")
    title_suffix = f" - {month_name}-{year}" if month and year else ""
    
    # Crea il bar chart
    fig = px.bar(
        type_totals,
        x='tipologia',
        y='totale',
        title=f"Distribuzione per tipologia di accoglienza{title_suffix}",
        labels={'totale': 'Numero migranti', 'tipologia': 'Tipologia'},
        color='tipologia',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    # Miglioramenti per tesi
    fig.update_layout(
        font=dict(size=12),
        plot_bgcolor='white',
        showlegend=False,
        height=400
    )
    
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    
    return fig

def create_daily_heatmap(df):
    """Crea una heatmap per la distribuzione degli sbarchi per giorno del mese"""
    if df.empty or 'giorno' not in df.columns:
        return None
    
    # Prepara i dati per la heatmap
    df['giorno'] = pd.to_numeric(df['giorno'], errors='coerce')
    df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
    df['anno'] = pd.to_datetime(df['data_riferimento']).dt.year
    
    # Assicurati di avere dati per più mesi (altrimenti la heatmap non funziona)
    if df['mese'].nunique() < 2:
        # Se abbiamo solo un mese, espandi artificialmente includendo mesi vuoti
        unique_years = df['anno'].unique()
        all_months = pd.DataFrame()
        
        for year in unique_years:
            for month in range(1, 13):
                month_data = df[(df['anno'] == year) & (df['mese'] == month)]
                if month_data.empty:
                    # Crea dati vuoti per questo mese
                    for day in range(1, 32):
                        all_months = pd.concat([all_months, pd.DataFrame({
                            'anno': [year],
                            'mese': [month],
                            'giorno': [day],
                            'migranti_sbarcati': [0]
                        })])
                else:
                    all_months = pd.concat([all_months, month_data[['anno', 'mese', 'giorno', 'migranti_sbarcati']]])
        
        df = all_months
    
    # Crea una pivot table per la heatmap
    heatmap_data = df.pivot_table(
        values='migranti_sbarcati',
        index=['anno', 'mese'],
        columns='giorno',
        aggfunc='sum',
        fill_value=0
    )
    
    # Crea etichette per l'asse y (anno-mese)
    y_labels = []
    for idx in heatmap_data.index:
        anno, mese = idx
        month_names = ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 
                       'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']
        y_labels.append(f"{month_names[mese-1]} {anno}")
    
    # Crea la heatmap
    fig = px.imshow(
        heatmap_data.values,
        labels=dict(x="Giorno del mese", y="Mese", color="Migranti sbarcati"),
        x=[str(i) for i in range(1, 32)],
        y=y_labels,
        title="Distribuzione mensile degli sbarchi (Heatmap)",
        color_continuous_scale='YlOrRd',
        aspect='auto'
    )
    
    # Miglioramenti per tesi
    fig.update_layout(
        font=dict(size=12),
        xaxis_title="Giorno del mese",
        yaxis_title="Periodo (Mese-Anno)",
        height=500
    )
    
    return fig
# Sidebar - Filtri e configurazioni
with st.sidebar:
    st.title("Filtri Dashboard")
    
    # Selezione dataset
    st.subheader("Dataset")
    available_tables = get_table_names()
    selected_table = st.selectbox(
        "Seleziona un dataset",
        available_tables,
        help="Scegli il dataset da analizzare"
    )
    
    # Filtro temporale - DIVERSO per tipo di dataset
    if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
        # Per dati cumulativi: Selezione mese singolo
        st.subheader("Seleziona mese di riferimento")
        
        # Ottieni anni e mesi disponibili per dati cumulativi
        years_months_data = get_available_years_months_for_cumulative()
        
        if not years_months_data:
            st.error("Nessun dato disponibile per selezionare il periodo.")
            st.stop()
        
        # Selettore anno
        available_years = list(years_months_data.keys())
        selected_year = st.selectbox(
            "Anno",
            options=available_years,
            index=0,
            help="Seleziona l'anno di riferimento",
            key="year_select_cumulative"
        )
        
        # Selettore mese
        month_names = {
            1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
            5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
            9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
        }
        
        available_months = years_months_data.get(selected_year, [])
        
        if not available_months:
            st.error(f"Nessun dato disponibile per l'anno {selected_year}")
            st.stop()
        
        selected_month_num = st.selectbox(
            "Mese",
            options=available_months,
            format_func=lambda x: month_names[x],
            help="Seleziona il mese di riferimento",
            key="month_select_cumulative"
        )
        
        # Per dati cumulativi, usa una data rappresentativa del mese (ultimo giorno)
        if selected_month_num == 12:
            start_date = date(selected_year, 12, 31)
            end_date = date(selected_year, 12, 31)
        else:
            end_date = date(selected_year, selected_month_num + 1, 1) - timedelta(days=1)
            start_date = end_date
        
        # Salva anno e mese in session state per uso nei titoli
        st.session_state.selected_year = selected_year
        st.session_state.selected_month_num = selected_month_num
        
    else:
        # Per dati_sbarchi: Filtro temporale con date di inizio e fine
        st.subheader("Filtra per data")
        
        # Date di default per dati_sbarchi
        default_start = date(2019, 9, 1)  # Primi dati disponibili per sbarchi
        default_end = date(2025, 10, 31)  # Data recente
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Data inizio",
                value=default_start,
                min_value=date(2017, 1, 1),
                max_value=date(2025, 12, 31)
            )
        with col2:
            end_date = st.date_input(
                "Data fine", 
                value=default_end,
                min_value=date(2017, 1, 1),
                max_value=date(2025, 12, 31)
            )
    
    # Filtri specifici per dataset
    if selected_table == 'dati_nazionalita':
        nazionalita_data = load_table_data('dati_nazionalita')
        
        # Mantiene l'ordine alfabetico nel menu
        nazionalita_list = sorted(nazionalita_data['nazionalita'].unique())
        
        # Titolo in grassetto usando markdown
        st.markdown("**Filtra per nazionalità**")
        
        # Container per i pulsanti
        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            if st.button("Seleziona tutto", key="select_all_naz", type="secondary", use_container_width=True):
                if 'selected_nazionalita' not in st.session_state:
                    st.session_state.selected_nazionalita = []
                st.session_state.selected_nazionalita = nazionalita_list
                st.rerun()
        
        with col_btn2:
            if st.button("Deseleziona tutto", key="deselect_all_naz", type="secondary", use_container_width=True):
                if 'selected_nazionalita' not in st.session_state:
                    st.session_state.selected_nazionalita = []
                st.session_state.selected_nazionalita = []
                st.rerun()
        
        # Usa session state per mantenere la selezione
        if 'selected_nazionalita' not in st.session_state:
            # Seleziona le prime 5 nazionalità per default
            if 'selected_year' in st.session_state:
                data_year = nazionalita_data[pd.to_datetime(nazionalita_data['data_riferimento']).dt.year == st.session_state.selected_year]
                totali_nazionalita = data_year.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
                top_5_nazionalita = totali_nazionalita.sort_values('migranti_sbarcati', ascending=False).head(5)['nazionalita'].tolist()
                st.session_state.selected_nazionalita = top_5_nazionalita
            else:
                st.session_state.selected_nazionalita = nazionalita_list[:5] if len(nazionalita_list) > 5 else nazionalita_list
        
        selected_nazionalita = st.multiselect(
            " ",
            options=nazionalita_list,
            default=st.session_state.selected_nazionalita,
            help="Seleziona le nazionalità da includere nell'analisi"
        )
        
        # Aggiorna session state
        if selected_nazionalita != st.session_state.selected_nazionalita:
            st.session_state.selected_nazionalita = selected_nazionalita
    
    elif selected_table == 'dati_accoglienza':
        accoglienza_data = load_table_data('dati_accoglienza')
        
        # Filtro per regione
        st.subheader("Filtra per regione")
        regioni_list = sorted(accoglienza_data['regione'].unique())
        
        # Container per i pulsanti regione
        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            if st.button("Seleziona tutto", key="select_all_reg", type="secondary", use_container_width=True):
                if 'selected_regioni' not in st.session_state:
                    st.session_state.selected_regioni = []
                st.session_state.selected_regioni = regioni_list
                st.rerun()
        
        with col_btn2:
            if st.button("Deseleziona tutto", key="deselect_all_reg", type="secondary", use_container_width=True):
                if 'selected_regioni' not in st.session_state:
                    st.session_state.selected_regioni = []
                st.session_state.selected_regioni = []
                st.rerun()
        
        # Usa session state per mantenere la selezione regione
        if 'selected_regioni' not in st.session_state:
            st.session_state.selected_regioni = regioni_list
        
        selected_regioni = st.multiselect(
            "Regioni",
            options=regioni_list,
            default=st.session_state.selected_regioni,
            help="Seleziona le regioni da includere nell'analisi"
        )
        
        # Aggiorna session state regione
        if selected_regioni != st.session_state.selected_regioni:
            st.session_state.selected_regioni = selected_regioni
        
        # Filtro per tipologia di accoglienza
        st.subheader("Filtra per tipologia di accoglienza")
        
        # Lista delle tipologie disponibili
        tipologie_list = ['Hot Spot', 'Centri Accoglienza', 'SIPROIMI/SAI']
        
        # Container per i pulsanti tipologia
        col_btn3, col_btn4 = st.columns([1, 1])
        with col_btn3:
            if st.button("Seleziona tutto", key="select_all_tip", type="secondary", use_container_width=True):
                if 'selected_tipologie' not in st.session_state:
                    st.session_state.selected_tipologie = []
                st.session_state.selected_tipologie = tipologie_list
                st.rerun()
        
        with col_btn4:
            if st.button("Deseleziona tutto", key="deselect_all_tip", type="secondary", use_container_width=True):
                if 'selected_tipologie' not in st.session_state:
                    st.session_state.selected_tipologie = []
                st.session_state.selected_tipologie = []
                st.rerun()
        
        # Usa session state per mantenere la selezione tipologia
        if 'selected_tipologie' not in st.session_state:
            st.session_state.selected_tipologie = tipologie_list
        
        selected_tipologie = st.multiselect(
            "Tipologie",
            options=tipologie_list,
            default=st.session_state.selected_tipologie,
            help="Seleziona le tipologie di accoglienza da includere nell'analisi"
        )
        
        # Aggiorna session state tipologia
        if selected_tipologie != st.session_state.selected_tipologie:
            st.session_state.selected_tipologie = selected_tipologie

# Header principale
st.title("Analisi del numero dei migranti sbarcati e dei migranti in accoglienza in Italia dal 2017")
st.markdown("Analisi esplorativa dei dati estratti dai report del Ministero dell'Interno")

# Sezione Overview - Metriche principali
st.header("Overview Generale")

try:
    # Carica i dati con i filtri applicati
    filters = {}
    if selected_table == 'dati_nazionalita' and 'selected_nazionalita' in st.session_state:
        filters = {'nazionalita': st.session_state.selected_nazionalita}
    elif selected_table == 'dati_accoglienza' and 'selected_regioni' in st.session_state:
        filters = {'regione': st.session_state.selected_regioni}
    
    # Query base
    filtered_data = query_filtered_data(
        table_name=selected_table,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        filters=filters if filters else None
    )
    
    # Per dati_sbarchi, ordina i dati grezzi dal giorno 1 all'ultimo giorno del mese
    if selected_table == 'dati_sbarchi' and not filtered_data.empty:
        # Assicurati che la colonna 'data_riferimento' sia datetime
        if 'data_riferimento' in filtered_data.columns:
            filtered_data['data_riferimento'] = pd.to_datetime(filtered_data['data_riferimento'])
            # Ordina per giorno numerico
            if 'giorno' in filtered_data.columns:
                filtered_data['giorno_num'] = pd.to_numeric(filtered_data['giorno'], errors='coerce')
                filtered_data = filtered_data.sort_values('giorno_num')
    
    # Per dati cumulativi, filtra per mese/anno specifico
    if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
        if 'data_riferimento' in filtered_data.columns:
            filtered_data['data_riferimento'] = pd.to_datetime(filtered_data['data_riferimento'])
            if 'selected_year' in st.session_state and 'selected_month_num' in st.session_state:
                filtered_data = filtered_data[
                    (filtered_data['data_riferimento'].dt.year == st.session_state.selected_year) &
                    (filtered_data['data_riferimento'].dt.month == st.session_state.selected_month_num)
                ]
    
    if not filtered_data.empty:
        # Determina la colonna valori in base al dataset
        if selected_table == 'dati_nazionalita':
            value_column = 'migranti_sbarcati'
            title_suffix = "migranti sbarcati"
            period_type = 'monthly'
            is_cumulative = True
        elif selected_table == 'dati_accoglienza':
            value_column = 'totale_accoglienza'
            title_suffix = "migranti in accoglienza"
            period_type = 'monthly'
            is_cumulative = True
        elif selected_table == 'dati_sbarchi':
            value_column = 'migranti_sbarcati'
            title_suffix = "sbarchi giornalieri"
            period_type = 'daily'
            is_cumulative = False
        else:
            value_column = filtered_data.select_dtypes(include=['number']).columns[0]
            title_suffix = "valori"
            period_type = 'monthly'
            is_cumulative = False
        
        # Calcola metriche (solo total)
        metrics = calculate_metrics(filtered_data, value_column, period_type, is_cumulative)
        
        # Display metriche - solo 1 colonna (Totale)
        if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
            month_name = month_names.get(st.session_state.get('selected_month_num', 1), "")
            year = st.session_state.get('selected_year', 2024)
            st.metric(
                label=f"Totale mese di {month_name}-{year}",
                value=f"{metrics['total']:,.0f}",
                help=f"Totale cumulativo dall'inizio del {year} fino a {month_name} {year}"
            )
        else:
            st.metric(
                label="Totale nel periodo selezionato",
                value=f"{metrics['total']:,.0f}",
                help=f"Totale {title_suffix} nel periodo {start_date} - {end_date}"
            )
        
        # Note informativa per dati cumulativi
        if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
            month_name = month_names.get(st.session_state.get('selected_month_num', 1), "")
            year = st.session_state.get('selected_year', 2024)
            st.info(f"""
            **Nota sui dati**: I valori mostrati sono **cumulativi annuali**.
            - **{month_name} {year}**: Totale dall'inizio dell'anno fino a {month_name} {year}
            - I dati riflettono l'accumulo progressivo durante l'anno, non il flusso mensile
            """)
        
        # Visualizzazioni specifiche per dataset
        st.header("Analisi Dettagliata")
        
        if selected_table == 'dati_nazionalita':
            st.subheader("Distribuzione per nazionalità")
            fig_bar = create_nationality_bar_chart(
                filtered_data, 
                year=st.session_state.get('selected_year'), 
                month=st.session_state.get('selected_month_num')
            )
            if fig_bar:
                st.plotly_chart(fig_bar, use_container_width=True)
        
        elif selected_table == 'dati_accoglienza':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Distribuzione regionale")
                
                # Preparazione dati per la mappa con filtri tipologie
                map_df = filtered_data.copy()
                
                # Applica filtri per tipologie se sono selezionate
                if 'selected_tipologie' in st.session_state and st.session_state.selected_tipologie:
                    tipologia_to_column = {
                        'Hot Spot': 'migranti_hot_spot',
                        'Centri Accoglienza': 'migranti_centri_accoglienza',
                        'SIPROIMI/SAI': 'migranti_siproimi_sai'
                    }
                    
                    selected_columns = [tipologia_to_column[tip] for tip in st.session_state.selected_tipologie 
                                       if tip in tipologia_to_column and tipologia_to_column[tip] in map_df.columns]
                    
                    if selected_columns:
                        map_df['totale_accoglienza'] = map_df[selected_columns].sum(axis=1)
                
                fig_map = create_simple_regional_map(
                    map_df, 
                    year=st.session_state.get('selected_year'), 
                    month=st.session_state.get('selected_month_num')
                )
                if fig_map:
                    st.plotly_chart(fig_map, use_container_width=True)
            
            with col2:
                st.subheader("Tipologie di accoglienza")
                # Passa le tipologie selezionate al grafico
                selected_tipologie = st.session_state.get('selected_tipologie', [])
                fig_bar = create_accommodation_bar_chart(
                    filtered_data, 
                    selected_types=selected_tipologie,
                    year=st.session_state.get('selected_year'), 
                    month=st.session_state.get('selected_month_num')
                )
                if fig_bar:
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        elif selected_table == 'dati_sbarchi':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Andamento giornaliero degli sbarchi")
                fig_column, daily_data = create_daily_column_chart(
                    filtered_data,
                    start_date,
                    end_date
                )
                if fig_column:
                    st.plotly_chart(fig_column, use_container_width=True)
            
            with col2:
                st.subheader("Distribuzione mensile (Heatmap)")
                fig_heatmap = create_daily_heatmap(filtered_data)
                if fig_heatmap:
                    st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Sezione dati grezzi per dati_sbarchi (ordinati per giorno)
            with st.expander("Dati Grezzi"):
                if daily_data is not None:
                    # Ordina i dati per data_completa (che contiene la data completa giorno-mese-anno)
                    display_data = daily_data.sort_values('data_completa').copy()
                    display_data = display_data.rename(columns={
                        'data_completa': 'Data',
                        'migranti_sbarcati': 'Migranti Sbarcati'
                    })
                    st.dataframe(display_data, use_container_width=True)
                    
                    # Opzione download
                    csv = display_data.to_csv(index=False)
                    st.download_button(
                        label="Scarica CSV",
                        data=csv,
                        file_name=f"{selected_table}_{start_date}_{end_date}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("Nessun dato disponibile per il periodo selezionato")
        
        # Sezione dati grezzi per dati_nazionalita e dati_accoglienza
        if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
            with st.expander("Dati Grezzi"):
                st.dataframe(filtered_data, use_container_width=True)
                
                # Opzione download
                csv = filtered_data.to_csv(index=False)
                st.download_button(
                    label="Scarica CSV",
                    data=csv,
                    file_name=f"{selected_table}_{start_date}_{end_date}.csv",
                    mime="text/csv"
                )
    
    else:
        st.warning("Nessun dato disponibile per i filtri selezionati")
        
except Exception as e:
    st.error(f"Errore nell'elaborazione dei dati: {str(e)}")
    st.info("Controlla i log di Streamlit Cloud per maggiori dettagli")

# Footer informativo
st.markdown("---")
ultima_data, ultimo_file = get_ultimo_aggiornamento()
st.markdown(
    f"""
    **Info:**
    - Dati estratti dal Cruscotto statistico del Ministero dell'Interno (2017-2025)
    - https://libertaciviliimmigrazione.dlci.interno.gov.it/documentazione/dati-e-statistiche/cruscotto-statistico-giornaliero
    - Ultimo aggiornamento: "{ultimo_file}"
    
    **Repository GitHub:** [MDA_2025_progetto_tesi](https://github.com/paoloRi/MDA_2025_progetto_tesi)
    """
)
