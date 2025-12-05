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

@st.cache_data(ttl=3600)
def get_temporal_coverage(table_name):
    """Restituisce la copertura temporale dei dati"""
    return database.get_temporal_coverage(table_name)

# Inizializzazione session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Funzione per ottenere anni e mesi disponibili
@st.cache_data(ttl=3600)
def get_available_years_months():
    """Restituisce gli anni e mesi disponibili in tutti i dataset"""
    years_months = {}
    
    try:
        for table_name in get_table_names():
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
            'total': 0,
            'avg_per_period': 0,
            'max_value': 0,
            'min_value': 0,
            'std_dev': 0,
            'median': 0
        }
    
    # Per dati cumulativi (singolo mese), il totale è il valore di quel mese
    if is_cumulative and period_type == 'monthly':
        if len(df) > 0:
            metrics = {
                'total': df[value_column].iloc[0] if len(df) == 1 else df[value_column].sum(),
                'avg_per_period': df[value_column].iloc[0] if len(df) == 1 else df[value_column].mean(),
                'max_value': df[value_column].max(),
                'min_value': df[value_column].min(),
                'std_dev': df[value_column].std(),
                'median': df[value_column].median()
            }
        else:
            metrics = {
                'total': 0,
                'avg_per_period': 0,
                'max_value': 0,
                'min_value': 0,
                'std_dev': 0,
                'median': 0
            }
    else:
        # Per dati non cumulativi o flussi giornalieri
        metrics = {
            'total': df[value_column].sum(),
            'avg_per_period': df[value_column].mean(),
            'max_value': df[value_column].max(),
            'min_value': df[value_column].min(),
            'std_dev': df[value_column].std(),
            'median': df[value_column].median()
        }
        
        # Calcola la media giornaliera se i dati sono giornalieri
        if period_type == 'daily' and 'data_riferimento' in df.columns:
            days_count = df['data_riferimento'].nunique()
            if days_count > 0:
                metrics['avg_daily'] = metrics['total'] / days_count
    
    return metrics

def create_time_series_chart(df, date_column, value_column, title):
    """Crea un grafico temporale"""
    if df.empty:
        return None
    
    df_sorted = df.sort_values(date_column)
    fig = px.line(
        df_sorted, 
        x=date_column, 
        y=value_column,
        title=title,
        labels={value_column: 'Numero migranti', date_column: 'Data'}
    )
    fig.update_traces(line=dict(width=3))
    return fig

def create_nationality_trend_chart(df, top_n=3, year=None, month=None):
    """Crea un line chart con al massimo 3 nazionalità per un mese specifico"""
    if df.empty or 'nazionalita' not in df.columns:
        return None
    
    # Calcola le top N nazionalità per il periodo selezionato
    top_nationalities = df.groupby('nazionalita')['migranti_sbarcati'].sum().nlargest(top_n).index.tolist()
    
    # Filtra solo le top nazionalità
    df_top = df[df['nazionalita'].isin(top_nationalities)]
    
    # Prepara titolo con mese e anno
    month_names = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names.get(month, "")
    title_suffix = f" - {month_name} {year}" if month and year else ""
    
    # Crea il line chart con una linea per ogni nazionalità
    fig = px.line(
        df_top,
        x='data_riferimento',
        y='migranti_sbarcati',
        color='nazionalita',
        title=f"Trend delle prime {top_n} nazionalità per numero di sbarchi{title_suffix}",
        labels={'migranti_sbarcati': 'Migranti sbarcati', 'data_riferimento': 'Data'},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    # Miglioramenti formattazione per tesi
    fig.update_layout(
        font=dict(size=12, family='Arial'),
        plot_bgcolor='white',
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    fig.update_traces(line=dict(width=2))
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    
    return fig

def create_nationality_bar_chart(df, top_n=10, year=None, month=None):
    """Crea un bar chart ordinato per le nazionalità"""
    if df.empty:
        return None
    
    # Calcola i totali e ordina
    nationality_totals = df.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
    nationality_totals = nationality_totals.sort_values('migranti_sbarcati', ascending=False).head(top_n)
    
    # Prepara titolo con mese e anno
    month_names = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names.get(month, "")
    title_suffix = f" - {month_name} {year}" if month and year else ""
    
    fig = px.bar(
        nationality_totals,
        x='migranti_sbarcati',
        y='nazionalita',
        orientation='h',
        title=f"Top {top_n} nazionalità per numero di sbarchi{title_suffix}",
        labels={'migranti_sbarcati': 'Totale migranti sbarcati', 'nazionalita': 'Nazionalità'},
        color='migranti_sbarcati',
        color_continuous_scale='Viridis_r'
    )
    
    # Miglioramenti per tesi
    fig.update_layout(
        font=dict(size=12),
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='white',
        height=400
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    
    return fig

def create_daily_heatmap(df, year=None, month=None):
    """Crea una heatmap per la distribuzione degli sbarchi per giorno del mese"""
    if df.empty or 'giorno' not in df.columns:
        return None
    
    # Prepara i dati per la heatmap
    df['giorno'] = pd.to_numeric(df['giorno'], errors='coerce')
    df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
    
    # Crea una pivot table per la heatmap
    heatmap_data = df.pivot_table(
        values='migranti_sbarcati',
        index='mese',
        columns='giorno',
        aggfunc='sum',
        fill_value=0
    )
    
    # Nomi dei mesi
    month_names = ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 
                   'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']
    
    # Prepara titolo con mese e anno
    month_name = month_names[month-1] if month and 1 <= month <= 12 else ""
    title_suffix = f" - {month_name} {year}" if month and year else ""
    
    # Crea la heatmap
    fig = px.imshow(
        heatmap_data,
        labels=dict(x="Giorno del mese", y="Mese", color="Migranti sbarcati"),
        x=[str(i) for i in range(1, 32)],
        y=[month_names[i-1] for i in heatmap_data.index],
        title=f"Distribuzione mensile degli sbarchi (Heatmap){title_suffix}",
        color_continuous_scale='YlOrRd',
        aspect='auto'
    )
    
    # Miglioramenti per tesi
    fig.update_layout(
        font=dict(size=12),
        xaxis_title="Giorno del mese",
        yaxis_title="Mese"
    )
    
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
    title_suffix = f" - {month_name} {year}" if month and year else ""
    
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

def create_accommodation_bar_chart(df, year=None, month=None):
    """Crea un bar chart per le tipologie di accoglienza"""
    if df.empty:
        return None
    
    # Definisci le colonne delle tipologie
    type_columns = ['migranti_hot_spot', 'migranti_centri_accoglienza', 'migranti_siproimi_sai']
    
    # Filtra solo le colonne disponibili
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
    month_names = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    
    month_name = month_names.get(month, "")
    title_suffix = f" - {month_name} {year}" if month and year else ""
    
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
        showlegend=False
    )
    
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    
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
    
    # Filtro temporale - Selezione mese singolo
    st.subheader("Seleziona mese di riferimento")
    
    # Ottieni anni e mesi disponibili
    years_months_data = get_available_years_months()
    
    if not years_months_data:
        st.error("Nessun dato disponibile per selezionare il periodo.")
        st.stop()
    
    # Selettore anno
    available_years = list(years_months_data.keys())
    selected_year = st.selectbox(
        "Anno",
        options=available_years,
        index=0,
        help="Seleziona l'anno di riferimento"
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
        help="Seleziona il mese di riferimento"
    )
    
    # Determina il periodo in base al dataset
    if selected_table == 'dati_sbarchi':
        # Per dati_sbarchi (giornalieri), usa tutto il mese
        start_date = date(selected_year, selected_month_num, 1)
        if selected_month_num == 12:
            end_date = date(selected_year, 12, 31)
        else:
            end_date = date(selected_year, selected_month_num + 1, 1) - timedelta(days=1)
    else:
        # Per dati cumulativi, usa una data rappresentativa del mese
        # Tipicamente l'ultimo giorno del mese
        if selected_month_num == 12:
            start_date = date(selected_year, 12, 31)
            end_date = date(selected_year, 12, 31)
        else:
            end_date = date(selected_year, selected_month_num + 1, 1) - timedelta(days=1)
            start_date = end_date
    
    # Filtri specifici per dataset
    if selected_table == 'dati_nazionalita':
        nazionalita_data = load_table_data('dati_nazionalita')
        
        # Calcola il totale per nazionalità per l'anno selezionato
        data_year = nazionalita_data[pd.to_datetime(nazionalita_data['data_riferimento']).dt.year == selected_year]
        totali_nazionalita = data_year.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
        
        # Ordina per totale (decrescente) e prende le prime 5
        top_5_nazionalita = totali_nazionalita.sort_values('migranti_sbarcati', ascending=False).head(5)['nazionalita'].tolist()
        
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
            st.session_state.selected_nazionalita = top_5_nazionalita
        
        selected_nazionalita = st.multiselect(
            " ",
            options=nazionalita_list,
            default=st.session_state.selected_nazionalita,
            help="Le prime 5 nazionalità sono selezionate di default in base al totale migranti sbarcati"
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
month_names = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
    5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
    9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
}

month_name = month_names.get(selected_month_num, "")
st.title(f"Analisi del numero dei migranti sbarcati e dei migranti in accoglienza in Italia - {month_name} {selected_year}")
st.markdown(f"Analisi esplorativa dei dati estratti dai report del Ministero dell'Interno - {month_name} {selected_year}")

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
    
    # Per dati cumulativi, filtra per mese/anno specifico
    if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
        if 'data_riferimento' in filtered_data.columns:
            filtered_data['data_riferimento'] = pd.to_datetime(filtered_data['data_riferimento'])
            filtered_data = filtered_data[
                (filtered_data['data_riferimento'].dt.year == selected_year) &
                (filtered_data['data_riferimento'].dt.month == selected_month_num)
            ]
    
    # Per dati_accoglienza, gestione filtri per tipologia
    if (selected_table == 'dati_accoglienza' and not filtered_data.empty and 
        'selected_tipologie' in st.session_state and st.session_state.selected_tipologie):
        
        # Mappa i nomi delle tipologie alle colonne del dataframe
        tipologia_to_column = {
            'Hot Spot': 'migranti_hot_spot',
            'Centri Accoglienza': 'migranti_centri_accoglienza',
            'SIPROIMI/SAI': 'migranti_siproimi_sai'
        }
        
        # Seleziona solo le colonne per le tipologie scelte
        selected_columns = [tipologia_to_column[tip] for tip in st.session_state.selected_tipologie 
                           if tip in tipologia_to_column and tipologia_to_column[tip] in filtered_data.columns]
        
        # Calcola il totale selezionato (somma delle colonne selezionate)
        if selected_columns:
            filtered_data['totale_accoglienza_selezionato'] = filtered_data[selected_columns].sum(axis=1)
            # Sovrascrivi la colonna totale_accoglienza con il totale selezionato
            filtered_data['totale_accoglienza'] = filtered_data['totale_accoglienza_selezionato']
    
    if not filtered_data.empty:
        # Determina la colonna valori in base al dataset
        if selected_table == 'dati_nazionalita':
            value_column = 'migranti_sbarcati'
            title_suffix = "migranti sbarcati (cumulativo annuale)"
            period_type = 'monthly'
            is_cumulative = True
        elif selected_table == 'dati_accoglienza':
            value_column = 'totale_accoglienza'
            title_suffix = "migranti in accoglienza (cumulativo annuale)"
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
        
        # Calcola metriche
        metrics = calculate_metrics(filtered_data, value_column, period_type, is_cumulative)
        
        # Display metriche
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
                help_text = f"Totale cumulativo dall'inizio del {selected_year} fino a {month_name}"
            else:
                help_text = f"Totale {title_suffix} nel periodo selezionato"
            
            st.metric(
                label="Totale",
                value=f"{metrics['total']:,.0f}",
                help=help_text
            )
        
        with col2:
            if period_type == 'daily' and 'avg_daily' in metrics:
                st.metric(
                    label="Media giornaliera",
                    value=f"{metrics['avg_daily']:,.1f}",
                    help=f"Media giornaliera di {title_suffix}"
                )
            else:
                help_text = f"Valore medio di {title_suffix}"
                if is_cumulative:
                    help_text = f"Valore cumulativo di {title_suffix} per {month_name}"
                
                st.metric(
                    label="Valore" if is_cumulative else "Media per periodo",
                    value=f"{metrics['avg_per_period']:,.1f}",
                    help=help_text
                )
        
        with col3:
            st.metric(
                label="Massimo",
                value=f"{metrics['max_value']:,.0f}",
                help=f"Valore massimo di {title_suffix}"
            )
        
        with col4:
            st.metric(
                label="Minimo",
                value=f"{metrics['min_value']:,.0f}",
                help=f"Valore minimo di {title_suffix}"
            )
        
        # Note informativa per dati cumulativi
        if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
            st.info(f"""
            **Nota sui dati**: I valori mostrati sono **cumulativi annuali**.
            - **{month_name} {selected_year}**: Totale dall'inizio dell'anno fino a {month_name} {selected_year}
            - I dati riflettono l'accumulo progressivo durante l'anno, non il flusso mensile
            """)
        
        # Visualizzazioni specifiche per dataset
        st.header("Analisi Dettagliata")
        
        if selected_table == 'dati_nazionalita':
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("Trend delle principali nazionalità")
                fig_trend = create_nationality_trend_chart(
                    filtered_data, 
                    top_n=3, 
                    year=selected_year, 
                    month=selected_month_num
                )
                if fig_trend:
                    st.plotly_chart(fig_trend, use_container_width=True)
            
            with col2:
                st.subheader("Distribuzione per nazionalità")
                fig_bar = create_nationality_bar_chart(
                    filtered_data, 
                    top_n=10, 
                    year=selected_year, 
                    month=selected_month_num
                )
                if fig_bar:
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        elif selected_table == 'dati_accoglienza':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Distribuzione regionale")
                fig_map = create_simple_regional_map(
                    filtered_data, 
                    year=selected_year, 
                    month=selected_month_num
                )
                if fig_map:
                    st.plotly_chart(fig_map, use_container_width=True)
            
            with col2:
                st.subheader("Tipologie di accoglienza")
                fig_bar = create_accommodation_bar_chart(
                    filtered_data, 
                    year=selected_year, 
                    month=selected_month_num
                )
                if fig_bar:
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        elif selected_table == 'dati_sbarchi':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Andamento giornaliero degli sbarchi")
                fig_trend = create_time_series_chart(
                    filtered_data,
                    'data_riferimento',
                    value_column,
                    f"Andamento sbarchi giornalieri - {month_name} {selected_year}"
                )
                if fig_trend:
                    st.plotly_chart(fig_trend, use_container_width=True)
            
            with col2:
                st.subheader("Distribuzione mensile (Heatmap)")
                fig_heatmap = create_daily_heatmap(
                    filtered_data, 
                    year=selected_year, 
                    month=selected_month_num
                )
                if fig_heatmap:
                    st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # Sezione dati grezzi
        with st.expander("Dati Grezzi"):
            st.dataframe(filtered_data, use_container_width=True)
            
            # Opzione download
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label="Scarica CSV",
                data=csv,
                file_name=f"{selected_table}_{selected_year}_{selected_month_num:02d}.csv",
                mime="text/csv"
            )
    
    else:
        st.warning(f"Nessun dato disponibile per {month_name} {selected_year}")
        
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
    - **Nota**: I dati di nazionalità e accoglienza sono cumulativi annuali
    - **Nota**: I dati di sbarchi sono flussi giornalieri
    
    **Repository GitHub:** [MDA_2025_progetto_tesi](https://github.com/paoloRi/MDA_2025_progetto_tesi)
    """
)
