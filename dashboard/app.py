# dashboard/app.py
"""
Dashboard Streamlit per l'analisi dei dati migrazione
Integrazione con il sistema Parquet Database esistente
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
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

# Funzioni di utilità per l'analisi
def calculate_metrics(df, value_column):
    """Calcola metriche principali da un DataFrame"""
    if df.empty:
        return {
            'total': 0,
            'avg_per_period': 0,
            'max_value': 0,
            'min_value': 0
        }
    
    return {
        'total': df[value_column].sum(),
        'avg_per_period': df[value_column].mean(),
        'max_value': df[value_column].max(),
        'min_value': df[value_column].min()
    }

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

def create_bar_chart(df, category_column, value_column, title, top_n=10):
    """Crea un grafico a barre"""
    if df.empty:
        return None
    
    # Seleziona le top N categorie
    top_categories = df.groupby(category_column)[value_column].sum().nlargest(top_n)
    df_top = df[df[category_column].isin(top_categories.index)]
    
    fig = px.bar(
        df_top,
        x=category_column,
        y=value_column,
        title=title,
        labels={value_column: 'Numero migranti', category_column: 'Categoria'}
    )
    return fig

def create_regional_map(df):
    """Crea una mappa delle regioni italiane"""
    if df.empty or 'regione' not in df.columns:
        return None
    
    # Coordinate delle 20 regioni italiane
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
    
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        size="totale_accoglienza",
        color="totale_accoglienza",
        hover_name="regione",
        hover_data={"totale_accoglienza": True},
        title="Distribuzione regionale migranti in accoglienza",
        size_max=30,
        zoom=5,
        color_continuous_scale=px.colors.sequential.Viridis
    )
    
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
    
    return fig

# Sidebar - Filtri e configurazioni
with st.sidebar:
    st.title(" Filtri Dashboard")
    
    # Selezione dataset
    st.subheader("Dataset")
    available_tables = get_table_names()
    selected_table = st.selectbox(
        "Seleziona dataset",
        available_tables,
        help="Scegli il dataset da analizzare"
    )
    
    # Filtro temporale
    st.subheader("Filtra per data")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data inizio",
            value=date(2017, 1, 1),
            min_value=date(2017, 1, 1),
            max_value=date(2025, 12, 31)
        )
    with col2:
        end_date = st.date_input(
            "Data fine", 
            value=date(2025, 10, 31),
            min_value=date(2017, 1, 1),
            max_value=date(2025, 12, 31)
        )
    
    # Filtri specifici per dataset
    st.subheader("Filtri Specifici")
    
    if selected_table == 'dati_nazionalita':
        nazionalita_data = load_table_data('dati_nazionalita')
        # Calcola le top 5 nazionalità per totale sbarchi
        totali_nazionalita = nazionalita_data.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
        top_5_nazionalita = totali_nazionalita.sort_values('migranti_sbarcati', ascending=False).head(5)['nazionalita'].tolist()
        
        # Lista ordinata alfabeticamente per le opzioni
        nazionalita_list = sorted(nazionalita_data['nazionalita'].unique())
        
        selected_nazionalita = st.multiselect(
            "Filtra per nazionalità",
            options=nazionalita_list,
            default=top_5_nazionalita,
            help="Le prime 5 nazionalità sono selezionate di default in base al totale migranti sbarcati"
        )
    
    elif selected_table == 'dati_accoglienza':
        accoglienza_data = load_table_data('dati_accoglienza')
        regioni_list = sorted(accoglienza_data['regione'].unique())
        selected_regioni = st.multiselect(
            "Regioni",
            options=regioni_list,
            default=regioni_list,
            help="Seleziona le regioni da includere nell'analisi"
        )
    
    # Pulsante applica filtri
    apply_filters = st.button(" Applica Filtri", type="primary")

# Header principale
st.title("Analisi del numero dei migranti sbarcati e dei migranti in accoglienza in Italia dal 2017")
st.markdown("Analisi esplorativa dei dati estratti dai report del Ministero dell'Interno")

# Sezione Overview - Metriche principali
st.header("Overview Generale")

try:
    # Carica i dati con i filtri applicati
    filters = {}
    if selected_table == 'dati_nazionalita' and 'selected_nazionalita' in locals():
        filters = {'nazionalita': selected_nazionalita}
    elif selected_table == 'dati_accoglienza' and 'selected_regioni' in locals():
        filters = {'regione': selected_regioni}
    
    filtered_data = query_filtered_data(
        table_name=selected_table,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        filters=filters if filters else None
    )
    
    if not filtered_data.empty:
        # Determina la colonna valori in base al dataset
        if selected_table == 'dati_nazionalita':
            value_column = 'migranti_sbarcati'
            title_suffix = "migranti sbarcati"
        elif selected_table == 'dati_accoglienza':
            value_column = 'totale_accoglienza'
            title_suffix = "migranti in accoglienza"
        elif selected_table == 'dati_sbarchi':
            value_column = 'migranti_sbarcati'
            title_suffix = "sbarchi giornalieri"
        else:
            value_column = filtered_data.select_dtypes(include=['number']).columns[0]
            title_suffix = "valori"
        
        # Calcola metriche
        metrics = calculate_metrics(filtered_data, value_column)
        
        # Display metriche
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Totale",
                value=f"{metrics['total']:,.0f}",
                help=f"Totale {title_suffix} nel periodo selezionato"
            )
        
        with col2:
            st.metric(
                label="Media per periodo",
                value=f"{metrics['avg_per_period']:,.1f}",
                help=f"Media {title_suffix} per periodo"
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
        
        # Visualizzazioni specifiche per dataset
        st.header(" Analisi Dettagliata")
        
        if selected_table == 'dati_nazionalita':
            # Trend temporale per nazionalità
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("Trend Temporale Sbarchi")
                fig_trend = create_time_series_chart(
                    filtered_data, 
                    'data_riferimento', 
                    value_column,
                    f"Trend sbarchi per nazionalità ({start_date} - {end_date})"
                )
                if fig_trend:
                    st.plotly_chart(fig_trend, use_container_width=True)
            
            with col2:
                st.subheader("Top Nazionalità")
                fig_bar = create_bar_chart(
                    filtered_data,
                    'nazionalita',
                    value_column,
                    "Distribuzione per nazionalità"
                )
                if fig_bar:
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        elif selected_table == 'dati_accoglienza':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Distribuzione Regionale")
                # Mappa o grafico a barre per regioni
                fig_map = create_regional_map(filtered_data)
                if fig_map:
                    st.plotly_chart(fig_map, use_container_width=True)
                else:
                    # Fallback a grafico a barre
                    fig_bar = create_bar_chart(
                        filtered_data,
                        'regione',
                        value_column,
                        "Distribuzione per regione"
                    )
                    if fig_bar:
                        st.plotly_chart(fig_bar, use_container_width=True)
            
            with col2:
                st.subheader("Tipologie Accoglienza")
                # Somma per tipologia di accoglienza
                acc_cols = ['migranti_hot_spot', 'migranti_centri_accoglienza', 'migranti_siproimi_sai']
                available_cols = [col for col in acc_cols if col in filtered_data.columns]
                
                if available_cols:
                    tipologie_data = filtered_data[available_cols].sum().reset_index()
                    tipologie_data.columns = ['tipologia', 'totale']
                    
                    fig_pie = px.pie(
                        tipologie_data,
                        values='totale',
                        names='tipologia',
                        title="Distribuzione per tipologia accoglienza"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
        
        elif selected_table == 'dati_sbarchi':
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Sbarchi Giornalieri")
                fig_trend = create_time_series_chart(
                    filtered_data,
                    'data_riferimento',
                    value_column,
                    "Andamento sbarchi giornalieri"
                )
                if fig_trend:
                    st.plotly_chart(fig_trend, use_container_width=True)
            
            with col2:
                st.subheader("Distribuzione per Giorno del Mese")
                if 'giorno' in filtered_data.columns:
                    giorno_stats = filtered_data.groupby('giorno')[value_column].sum().reset_index()
                    fig_bar = px.bar(
                        giorno_stats,
                        x='giorno',
                        y=value_column,
                        title="Sbarchi per giorno del mese"
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        # Sezione dati grezzi
        with st.expander(" Dati Grezzi"):
            st.dataframe(filtered_data, use_container_width=True)
            
            # Opzione download
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label=" Scarica CSV",
                data=csv,
                file_name=f"{selected_table}_{start_date}_{end_date}.csv",
                mime="text/csv"
            )
    
    else:
        st.warning(" Nessun dato disponibile per i filtri selezionati")
        
except Exception as e:
    st.error(f" Errore nell'elaborazione dei dati: {str(e)}")
    st.info(" Controlla i log di Streamlit Cloud per maggiori dettagli")

# Footer informativo
st.markdown("---")
ultima_data, ultimo_file = get_ultimo_aggiornamento()
st.markdown(
    """
    **Info:**
    - Dati estratti dal Cruscotto statistico del Ministero dell'Interno (2017-2025)
    - https://libertaciviliimmigrazione.dlci.interno.gov.it/documentazione/dati-e-statistiche/cruscotto-statistico-giornaliero
    - Ultimo aggiornamento: "{ultimo_file}"
    
    **Repository GitHub:** [MDA_2025_progetto_tesi](https://github.com/tuo-username/MDA_2025_progetto_tesi)
    """
)
