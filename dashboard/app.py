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
    from utils.parquet_database import database, get_table_names, quick_query
    IMPORT_SUCCESS = True
    print("Import di parquet_database riuscito")
    
except ImportError as e:
    IMPORT_SUCCESS = False
    st.error(f"""
    **ERRORE CRITICO NELL'IMPORTAZIONE DEL MODULO 'parquet_database'**
    **Dettaglio tecnico:** {e}
    **Informazioni di sistema Streamlit Cloud:**
    - Directory di esecuzione: `{os.getcwd()}`
    - Project root: `{project_root}`
    - Utils path: `{project_root / 'utils'}`
    - Utils esiste: `{(project_root / 'utils').exists()}`
    - sys.path (primi 5): `{sys.path[:5]}`
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

# NUOVE FUNZIONI PER CALCOLO FLUSSI
@st.cache_data(ttl=3600)
def calculate_monthly_flow(df, group_columns, value_column):
    """
    Calcola il flusso mensile dai dati cumulativi annuali
    Gestisce i mesi mancanti prendendo l'ultimo dato disponibile (forward fill)
    """
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['anno'] = pd.to_datetime(df['data_riferimento']).dt.year
    df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
    
    # Ordina per gruppo, anno e mese
    df = df.sort_values(group_columns + ['anno', 'mese'])
    
    # Forward fill per gestire mesi mancanti
    df['valore_ffill'] = df.groupby(group_columns)[value_column].ffill()
    
    # Calcola flusso mensile (differenza rispetto al mese precedente)
    df['flusso_mensile'] = df.groupby(group_columns)['valore_ffill'].diff()
    
    # Per il primo mese di ogni anno/gruppo, il flusso = valore cumulativo
    df['flusso_mensile'] = df.groupby(group_columns)['flusso_mensile'].transform(
        lambda x: x.fillna(x.iloc[0]) if not x.empty else x
    )
    
    # Rimuovi righe senza dati
    df = df.dropna(subset=['valore_ffill'])
    
    return df

@st.cache_data(ttl=3600)
def get_available_years_months_for_cumulative():
    """Restituisce gli anni e mesi disponibili per dati cumulativi (nazionalità e accoglienza)"""
    years_months = {}
    
    try:
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
    
    sorted_years_months = {}
    for year in sorted(years_months.keys(), reverse=True):
        sorted_years_months[year] = sorted(years_months[year], reverse=True)
    
    return sorted_years_months

# FUNZIONI PER LE NUOVE VISUALIZZAZIONI FLUSSI
def create_nationality_trend_chart(df, selected_nationalities, start_date, end_date):
    """Crea un line chart per l'andamento temporale delle nazionalità selezionate (flussi)"""
    if df.empty or len(selected_nationalities) == 0:
        return None
    
    # Calcola flussi mensili
    flow_data = calculate_monthly_flow(
        df[df['nazionalita'].isin(selected_nationalities)],
        group_columns=['nazionalita'],
        value_column='migranti_sbarcati'
    )
    
    if flow_data.empty:
        return None
    
    # Filtra per periodo
    flow_data['data_completa'] = pd.to_datetime(
        flow_data['anno'].astype(str) + '-' + 
        flow_data['mese'].astype(str) + '-01'
    )
    flow_data = flow_data[
        (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
        (flow_data['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    if flow_data.empty:
        return None
    
    fig = px.line(
        flow_data,
        x='data_completa',
        y='flusso_mensile',
        color='nazionalita',
        title="Andamento mensile degli sbarchi per nazionalità (flusso)",
        labels={
            'flusso_mensile': 'Migranti sbarcati (flusso mensile)', 
            'data_completa': 'Mese',
            'nazionalita': 'Nazionalità'
        },
        markers=True
    )
    
    fig.update_layout(
        font=dict(size=12, family='Arial'),
        plot_bgcolor='white',
        showlegend=True,
        height=500,
        xaxis=dict(
            tickformat='%b %Y',
            tickangle=45,
            showgrid=True,
            gridwidth=0.5,
            gridcolor='LightGrey'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=0.5,
            gridcolor='LightGrey'
        ),
        hovermode='x unified'
    )
    
    return fig

def create_nationality_bar_chart(df, start_date, end_date, selected_nationalities):
    """Crea un bar chart ordinato per flusso cumulato nel periodo"""
    if df.empty:
        return None
    
    # Calcola flussi mensili
    flow_data = calculate_monthly_flow(
        df[df['nazionalita'].isin(selected_nationalities)],
        group_columns=['nazionalita'],
        value_column='migranti_sbarcati'
    )
    
    if flow_data.empty:
        return None
    
    # Filtra per periodo
    flow_data['data_completa'] = pd.to_datetime(
        flow_data['anno'].astype(str) + '-' + 
        flow_data['mese'].astype(str) + '-01'
    )
    flow_data = flow_data[
        (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
        (flow_data['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    # Calcola totale flusso per nazionalità nel periodo
    nationality_totals = flow_data.groupby('nazionalita')['flusso_mensile'].sum().reset_index()
    nationality_totals = nationality_totals.sort_values('flusso_mensile', ascending=False)
    
    # Togli valori negativi (imposta a 0 per visualizzazione)
    nationality_totals['flusso_mensile'] = nationality_totals['flusso_mensile'].clip(lower=0)
    
    # Prepara titolo
    start_str = start_date.strftime('%b %Y')
    end_str = end_date.strftime('%b %Y')
    
    fig = px.bar(
        nationality_totals,
        x='flusso_mensile',
        y='nazionalita',
        orientation='h',
        title=f"Flusso cumulato per nazionalità ({start_str} - {end_str})",
        labels={'flusso_mensile': 'Totale flusso nel periodo', 'nazionalita': 'Nazionalità'},
        color='flusso_mensile',
        color_continuous_scale='Viridis_r'
    )
    
    fig.update_layout(
        font=dict(size=12),
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='white',
        height=600,
        xaxis=dict(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    )
    
    return fig

def create_accommodation_pie_chart(df, selected_types, start_date, end_date):
    """Crea un pie chart per le tipologie di accoglienza (flusso cumulato nel periodo)"""
    if df.empty:
        return None
    
    # Mappa colonne
    type_columns = {
        'Hot Spot': 'migranti_hot_spot',
        'Centri Accoglienza': 'migranti_centri_accoglienza',
        'SIPROIMI/SAI': 'migranti_siproimi_sai'
    }
    
    # Filtra colonne selezionate
    selected_cols = [type_columns[tip] for tip in selected_types if tip in type_columns]
    
    # Calcola flussi per ogni regione e somma le tipologie
    flow_data_list = []
    for col in selected_cols:
        if col in df.columns:
            col_flow = calculate_monthly_flow(
                df[['data_riferimento', 'regione', col]],
                group_columns=['regione'],
                value_column=col
            )
            if not col_flow.empty:
                # Trova il nome della tipologia
                tip_name = [k for k, v in type_columns.items() if v == col][0]
                col_flow['tipologia'] = tip_name
                col_flow['flusso'] = col_flow['flusso_mensile']
                flow_data_list.append(col_flow[['anno', 'mese', 'tipologia', 'flusso']])
    
    if not flow_data_list:
        return None
    
    flow_data = pd.concat(flow_data_list, ignore_index=True)
    
    # Filtra per periodo
    flow_data['data_completa'] = pd.to_datetime(
        flow_data['anno'].astype(str) + '-' + 
        flow_data['mese'].astype(str) + '-01'
    )
    flow_data = flow_data[
        (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
        (flow_data['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    # Somma flussi per tipologia
    pie_data = flow_data.groupby('tipologia')['flusso'].sum().reset_index()
    pie_data['flusso'] = pie_data['flusso'].clip(lower=0)  # Togli valori negativi
    
    # Prepara titolo
    start_str = start_date.strftime('%b %Y')
    end_str = end_date.strftime('%b %Y')
    
    fig = px.pie(
        pie_data,
        values='flusso',
        names='tipologia',
        title=f"Distribuzione per tipologia di accoglienza (flusso cumulato {start_str}-{end_str})",
        hole=0.3,
        color='tipologia',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        font=dict(size=12),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        height=500
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig

def create_regional_flow_map(df, selected_types, start_date, end_date):
    """Crea mappa regionale con flusso cumulato nel periodo"""
    if df.empty:
        return None
    
    # Mappa colonne tipologie
    type_columns = {
        'Hot Spot': 'migranti_hot_spot',
        'Centri Accoglienza': 'migranti_centri_accoglienza',
        'SIPROIMI/SAI': 'migranti_siproimi_sai'
    }
    
    # Somma le colonne selezionate per ottenere totale regionale
    selected_cols = [type_columns[tip] for tip in selected_types if tip in type_columns]
    
    if not selected_cols:
        return None
    
    # Calcola flussi per regione (sommando tutte le tipologie)
    flow_data = calculate_monthly_flow(
        df,
        group_columns=['regione'],
        value_column='totale_accoglienza'
    )
    
    if flow_data.empty:
        return None
    
    # Filtra per periodo
    flow_data['data_completa'] = pd.to_datetime(
        flow_data['anno'].astype(str) + '-' + 
        flow_data['mese'].astype(str) + '-01'
    )
    flow_data = flow_data[
        (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
        (flow_data['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    # Calcola flusso totale cumulato per regione
    regional_totals = flow_data.groupby('regione')['flusso_mensile'].sum().reset_index()
    regional_totals['flusso_mensile'] = regional_totals['flusso_mensile'].clip(lower=0)
    
    # Coordinate delle regioni italiane
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
    
    # Prepara dati per la mappa
    map_data = []
    for regione, coords in region_coordinates.items():
        region_flow = regional_totals[regional_totals['regione'] == regione]
        if not region_flow.empty:
            total_flow = region_flow['flusso_mensile'].iloc[0]
            map_data.append({
                'regione': regione,
                'lat': coords[0],
                'lon': coords[1],
                'flusso_totale': total_flow
            })
    
    if not map_data:
        return None
    
    map_df = pd.DataFrame(map_data)
    
    # Prepara titolo
    start_str = start_date.strftime('%b %Y')
    end_str = end_date.strftime('%b %Y')
    
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        size="flusso_totale",
        color="flusso_totale",
        hover_name="regione",
        hover_data={
            "flusso_totale": ':.0f',
            "lat": False,
            "lon": False
        },
        title=f"Flusso regionale migranti in accoglienza ({start_str} - {end_str})",
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
            title="Flusso totale",
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

# FUNZIONI PER VISUALIZZAZIONE STOCK
def create_nationality_stock_trend_chart(df, selected_nationalities, start_date, end_date):
    """Crea un line chart per l'andamento temporale dei dati stock originali"""
    if df.empty or len(selected_nationalities) == 0:
        return None
    
    # Filtra per nazionalità selezionate
    df = df[df['nazionalita'].isin(selected_nationalities)].copy()
    
    # Estrai data e ordina
    df['data_completa'] = pd.to_datetime(df['data_riferimento'])
    df = df.sort_values('data_completa')
    
    # Filtra per periodo
    df = df[
        (df['data_completa'] >= pd.Timestamp(start_date)) & 
        (df['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    if df.empty:
        return None
    
    fig = px.line(
        df,
        x='data_completa',
        y='migranti_sbarcati',
        color='nazionalita',
        title="Andamento stock cumulativo per nazionalità",
        labels={
            'migranti_sbarcati': 'Migranti sbarcati (stock cumulativo)', 
            'data_completa': 'Mese',
            'nazionalita': 'Nazionalità'
        },
        markers=True
    )
    
    fig.update_layout(
        font=dict(size=12, family='Arial'),
        plot_bgcolor='white',
        showlegend=True,
        height=500,
        xaxis=dict(
            tickformat='%b %Y',
            tickangle=45,
            showgrid=True,
            gridwidth=0.5,
            gridcolor='LightGrey'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=0.5,
            gridcolor='LightGrey'
        ),
        hovermode='x unified'
    )
    
    return fig

def create_nationality_stock_bar_chart(df, start_date, end_date, selected_nationalities):
    """Crea un bar chart per dati stock (cumulativi all'ultimo mese del periodo)"""
    if df.empty:
        return None
    
    # Filtra per nazionalità selezionate
    df = df[df['nazionalita'].isin(selected_nationalities)].copy()
    
    # Estrai data e ordina
    df['data_completa'] = pd.to_datetime(df['data_riferimento'])
    df = df.sort_values('data_completa')
    
    # Filtra per periodo
    df = df[
        (df['data_completa'] >= pd.Timestamp(start_date)) & 
        (df['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    if df.empty:
        return None
    
    # Prendi l'ultimo mese disponibile nel periodo
    last_date = df['data_completa'].max()
    last_month_data = df[df['data_completa'] == last_date]
    
    # Calcola stock cumulativo per nazionalità nell'ultimo mese
    nationality_totals = last_month_data.groupby('nazionalita')['migranti_sbarcati'].sum().reset_index()
    nationality_totals = nationality_totals.sort_values('migranti_sbarcati', ascending=False)
    
    # Prepara titolo
    last_date_str = last_date.strftime('%b %Y')
    
    fig = px.bar(
        nationality_totals,
        x='migranti_sbarcati',
        y='nazionalita',
        orientation='h',
        title=f"Stock cumulativo per nazionalità al {last_date_str}",
        labels={'migranti_sbarcati': 'Stock cumulativo', 'nazionalita': 'Nazionalità'},
        color='migranti_sbarcati',
        color_continuous_scale='Viridis_r'
    )
    
    fig.update_layout(
        font=dict(size=12),
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='white',
        height=600,
        xaxis=dict(showgrid=True, gridwidth=0.5, gridcolor='LightGrey')
    )
    
    return fig

def create_regional_stock_map(df, selected_types, start_date, end_date):
    """Crea mappa regionale con stock cumulativo all'ultimo mese del periodo"""
    if df.empty:
        return None
    
    # Estrai data e ordina
    df = df.copy()
    df['data_completa'] = pd.to_datetime(df['data_riferimento'])
    df = df.sort_values('data_completa')
    
    # Filtra per periodo
    df = df[
        (df['data_completa'] >= pd.Timestamp(start_date)) & 
        (df['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    if df.empty:
        return None
    
    # Prendi l'ultimo mese disponibile nel periodo
    last_date = df['data_completa'].max()
    last_month_data = df[df['data_completa'] == last_date]
    
    # Somma le colonne selezionate per ottenere totale regionale
    type_columns = {
        'Hot Spot': 'migranti_hot_spot',
        'Centri Accoglienza': 'migranti_centri_accoglienza',
        'SIPROIMI/SAI': 'migranti_siproimi_sai'
    }
    
    selected_cols = [type_columns[tip] for tip in selected_types if tip in type_columns]
    
    if not selected_cols:
        return None
    
    # Calcola il totale per regione (sommando le tipologie selezionate)
    last_month_data['totale_stock'] = last_month_data[selected_cols].sum(axis=1)
    
    # Coordinate delle regioni italiane
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
    
    # Prepara dati per la mappa
    map_data = []
    for regione, coords in region_coordinates.items():
        region_stock = last_month_data[last_month_data['regione'] == regione]
        if not region_stock.empty:
            total_stock = region_stock['totale_stock'].iloc[0]
            map_data.append({
                'regione': regione,
                'lat': coords[0],
                'lon': coords[1],
                'stock_totale': total_stock
            })
    
    if not map_data:
        return None
    
    map_df = pd.DataFrame(map_data)
    
    # Prepara titolo
    last_date_str = last_date.strftime('%b %Y')
    
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        size="stock_totale",
        color="stock_totale",
        hover_name="regione",
        hover_data={
            "stock_totale": ':.0f',
            "lat": False,
            "lon": False
        },
        title=f"Stock regionale migranti in accoglienza al {last_date_str}",
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
            title="Stock totale",
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

def create_accommodation_stock_pie_chart(df, selected_types, start_date, end_date):
    """Crea un pie chart per le tipologie di accoglienza (stock all'ultimo mese del periodo)"""
    if df.empty:
        return None
    
    # Estrai data e ordina
    df = df.copy()
    df['data_completa'] = pd.to_datetime(df['data_riferimento'])
    df = df.sort_values('data_completa')
    
    # Filtra per periodo
    df = df[
        (df['data_completa'] >= pd.Timestamp(start_date)) & 
        (df['data_completa'] <= pd.Timestamp(end_date))
    ]
    
    if df.empty:
        return None
    
    # Prendi l'ultimo mese disponibile nel periodo
    last_date = df['data_completa'].max()
    last_month_data = df[df['data_completa'] == last_date]
    
    # Mappa colonne
    type_columns = {
        'Hot Spot': 'migranti_hot_spot',
        'Centri Accoglienza': 'migranti_centri_accoglienza',
        'SIPROIMI/SAI': 'migranti_siproimi_sai'
    }
    
    # Filtra colonne selezionate
    selected_cols = [type_columns[tip] for tip in selected_types if tip in type_columns]
    
    # Calcola il totale per tipologia nell'ultimo mese
    pie_data = []
    for tipologia, col in type_columns.items():
        if tipologia in selected_types and col in last_month_data.columns:
            total = last_month_data[col].sum()
            pie_data.append({'tipologia': tipologia, 'stock': total})
    
    if not pie_data:
        return None
    
    pie_df = pd.DataFrame(pie_data)
    
    # Prepara titolo
    last_date_str = last_date.strftime('%b %Y')
    
    fig = px.pie(
        pie_df,
        values='stock',
        names='tipologia',
        title=f"Distribuzione stock per tipologia di accoglienza al {last_date_str}",
        hole=0.3,
        color='tipologia',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        font=dict(size=12),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        height=500
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig

# FUNZIONI ESISTENTI (non modificate per dati_sbarchi)
def create_daily_column_chart(df, start_date, end_date):
    """Crea un column chart giornaliero per dati_sbarchi"""
    if df.empty or 'giorno' not in df.columns or 'data_riferimento' not in df.columns:
        return None, None
    
    try:
        df['anno'] = pd.to_datetime(df['data_riferimento']).dt.year
        df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
        df['giorno_num'] = pd.to_numeric(df['giorno'], errors='coerce')
        
        df['data_completa'] = pd.to_datetime(
            df['anno'].astype(str) + '-' + 
            df['mese'].astype(str) + '-' + 
            df['giorno_num'].astype(str)
        )
        
        df = df[(df['data_completa'] >= pd.Timestamp(start_date)) & 
                (df['data_completa'] <= pd.Timestamp(end_date))]
        
        if df.empty:
            return None, None
        
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_all_dates = pd.DataFrame({'data_completa': all_dates})
        
        df_merged = pd.merge(df_all_dates, df[['data_completa', 'migranti_sbarcati']], 
                            on='data_completa', how='left')
        df_merged['migranti_sbarcati'] = df_merged['migranti_sbarcati'].fillna(0)
        
        df_merged = df_merged.sort_values('data_completa')
        
        fig = px.bar(
            df_merged,
            x='data_completa',
            y='migranti_sbarcati',
            title=f"Sbarchi giornalieri ({start_date} - {end_date})",
            labels={'migranti_sbarcati': 'Migranti sbarcati', 'data_completa': 'Data'},
            color='migranti_sbarcati',
            color_continuous_scale='Viridis'
        )
        
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

def create_daily_heatmap(df):
    """Crea una heatmap per la distribuzione degli sbarchi per giorno del mese"""
    if df.empty or 'giorno' not in df.columns:
        return None
    
    df['giorno'] = pd.to_numeric(df['giorno'], errors='coerce')
    df['mese'] = pd.to_datetime(df['data_riferimento']).dt.month
    df['anno'] = pd.to_datetime(df['data_riferimento']).dt.year
    
    if df['mese'].nunique() < 2:
        unique_years = df['anno'].unique()
        all_months = pd.DataFrame()
        
        for year in unique_years:
            for month in range(1, 13):
                month_data = df[(df['anno'] == year) & (df['mese'] == month)]
                if month_data.empty:
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
    
    heatmap_data = df.pivot_table(
        values='migranti_sbarcati',
        index=['anno', 'mese'],
        columns='giorno',
        aggfunc='sum',
        fill_value=0
    )
    
    y_labels = []
    for idx in heatmap_data.index:
        anno, mese = idx
        month_names = ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 
                       'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']
        y_labels.append(f"{month_names[mese-1]} {anno}")
    
    fig = px.imshow(
        heatmap_data.values,
        labels=dict(x="Giorno del mese", y="Mese", color="Migranti sbarcati"),
        x=[str(i) for i in range(1, 32)],
        y=y_labels,
        title="Distribuzione mensile degli sbarchi (Heatmap)",
        color_continuous_scale='YlOrRd',
        aspect='auto'
    )
    
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
    
    # Filtro temporale - MODIFICATO per dati cumulativi
    if selected_table in ['dati_nazionalita', 'dati_accoglienza']:
        # Per dati cumulativi: Selezione intervallo di mesi
        st.subheader("Seleziona intervallo di mesi")
        
        # Ottieni anni e mesi disponibili per dati cumulativi
        years_months_data = get_available_years_months_for_cumulative()
        
        if not years_months_data:
            st.error("Nessun dato disponibile per selezionare il periodo.")
            st.stop()
        
        # Preset per periodo 2025 (per la tesi)
        st.markdown("**Preset periodi:**")
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            if st.button("2025 completo", key="preset_2025", type="secondary", use_container_width=True):
                st.session_state.start_year = 2025
                st.session_state.start_month = 1
                st.session_state.end_year = 2025
                st.session_state.end_month = 11
                st.rerun()
        
        with col_btn2:
            if st.button("Ultimi 12 mesi", key="preset_last12", type="secondary", use_container_width=True):
                # Trova l'ultimo mese disponibile
                last_year = max(years_months_data.keys())
                last_month = max(years_months_data[last_year])
                
                # Calcola 12 mesi prima
                if last_month > 1:
                    start_month = last_month - 1
                    start_year = last_year
                else:
                    start_month = 12
                    start_year = last_year - 1
                
                st.session_state.start_year = start_year
                st.session_state.start_month = start_month
                st.session_state.end_year = last_year
                st.session_state.end_month = last_month
                st.rerun()
        
        with col_btn3:
            if st.button("Tutto", key="preset_all", type="secondary", use_container_width=True):
                first_year = min(years_months_data.keys())
                first_month = min(years_months_data[first_year])
                last_year = max(years_months_data.keys())
                last_month = max(years_months_data[last_year])
                
                st.session_state.start_year = first_year
                st.session_state.start_month = first_month
                st.session_state.end_year = last_year
                st.session_state.end_month = last_month
                st.rerun()
        
        # Selettori anno/mese
        available_years = list(years_months_data.keys())
        month_names = {
            1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
            5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
            9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
        }
        
        # Inizializza session state per i filtri
        if 'start_year' not in st.session_state:
            st.session_state.start_year = 2025
            st.session_state.start_month = 1
            st.session_state.end_year = 2025
            st.session_state.end_month = 11
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Data inizio")
            start_year = st.selectbox(
                "Anno",
                options=available_years,
                index=available_years.index(st.session_state.start_year) if st.session_state.start_year in available_years else 0,
                help="Seleziona l'anno di inizio",
                key="start_year_select"
            )
            
            available_start_months = years_months_data.get(start_year, [])
            start_month = st.selectbox(
                "Mese",
                options=available_start_months,
                format_func=lambda x: month_names[x],
                index=available_start_months.index(st.session_state.start_month) if st.session_state.start_month in available_start_months else 0,
                help="Seleziona il mese di inizio",
                key="start_month_select"
            )
        
        with col2:
            st.subheader("Data fine")
            end_year = st.selectbox(
                "Anno",
                options=available_years,
                index=available_years.index(st.session_state.end_year) if st.session_state.end_year in available_years else len(available_years)-1,
                help="Seleziona l'anno di fine",
                key="end_year_select"
            )
            
            available_end_months = years_months_data.get(end_year, [])
            end_month = st.selectbox(
                "Mese",
                options=available_end_months,
                format_func=lambda x: month_names[x],
                index=available_end_months.index(st.session_state.end_month) if st.session_state.end_month in available_end_months else len(available_end_months)-1,
                help="Seleziona il mese di fine",
                key="end_month_select"
            )
        
        # Converti in date
        start_date = date(start_year, start_month, 1)
        end_date = date(end_year, end_month, 1) + timedelta(days=31)
        end_date = end_date.replace(day=1) - timedelta(days=1)
        
        # Salva in session state
        st.session_state.selected_start_date = start_date
        st.session_state.selected_end_date = end_date
        st.session_state.selected_year = start_year  # Per compatibilità con codice esistente
        st.session_state.selected_month_num = start_month  # Per compatibilità
        
    else:
        # Per dati_sbarchi: Filtro temporale con date di inizio e fine (NON MODIFICATO)
        st.subheader("Filtra per data")
        
        default_start = date(2019, 9, 1)
        default_end = date(2025, 10, 31)
        
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
    
    # Filtri specifici per dataset (NON MODIFICATI)
    if selected_table == 'dati_nazionalita':
        nazionalita_data = load_table_data('dati_nazionalita')
        nazionalita_list = sorted(nazionalita_data['nazionalita'].unique())
        
        st.markdown("**Filtra per nazionalità**")
        
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
        
        if 'selected_nazionalita' not in st.session_state:
            if 'start_year' in st.session_state:
                data_year = nazionalita_data[pd.to_datetime(nazionalita_data['data_riferimento']).dt.year == st.session_state.start_year]
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
        
        if selected_nazionalita != st.session_state.selected_nazionalita:
            st.session_state.selected_nazionalita = selected_nazionalita
    
    elif selected_table == 'dati_accoglienza':
        accoglienza_data = load_table_data('dati_accoglienza')
        
        # Filtro per regione
        st.subheader("Filtra per regione")
        regioni_list = sorted(accoglienza_data['regione'].unique())
        
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
        
        if 'selected_regioni' not in st.session_state:
            st.session_state.selected_regioni = regioni_list
        
        selected_regioni = st.multiselect(
            "Regioni",
            options=regioni_list,
            default=st.session_state.selected_regioni,
            help="Seleziona le regioni da includere nell'analisi"
        )
        
        if selected_regioni != st.session_state.selected_regioni:
            st.session_state.selected_regioni = selected_regioni
        
        # Filtro per tipologia di accoglienza
        st.subheader("Filtra per tipologia di accoglienza")
        tipologie_list = ['Hot Spot', 'Centri Accoglienza', 'SIPROIMI/SAI']
        
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
        
        if 'selected_tipologie' not in st.session_state:
            st.session_state.selected_tipologie = tipologie_list
        
        selected_tipologie = st.multiselect(
            "Tipologie",
            options=tipologie_list,
            default=st.session_state.selected_tipologie,
            help="Seleziona le tipologie di accoglienza da includere nell'analisi"
        )
        
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
    
    if not filtered_data.empty:
        # Determina tipo di dati
        is_cumulative = selected_table in ['dati_nazionalita', 'dati_accoglienza']
        is_sbarchi = selected_table == 'dati_sbarchi'
        
        # BOX INFORMATIVO PER DATI CUMULATIVI
        if is_cumulative:
            st.info("""
            **ANALISI FLUSSI MENSILI**  
            I dati originali sono cumulativi annuali. Il flusso netto mensile è ottenuto sottraendo il valore di ogni mese dal precedente.  
            **Metodologia:**  
            - Flusso mensile = valore del mese corrente - valore del mese precedente  
            - Mesi mancanti: utilizzato l'ultimo dato disponibile (forward fill)  
            - Valori negativi: possibili correzioni retroattive nei dati originali (consolidamento)
            - Errori nel processo di estrazione dei dati
            """)
        
        # Display metriche
        if is_cumulative:
            # Calcola flussi mensili
            group_columns = ['nazionalita'] if selected_table == 'dati_nazionalita' else ['regione']
            value_column = 'migranti_sbarcati' if selected_table == 'dati_nazionalita' else 'totale_accoglienza'
            
            flow_data = calculate_monthly_flow(
                filtered_data,
                group_columns=group_columns,
                value_column=value_column
            )
            
            if not flow_data.empty:
                # Filtra per periodo selezionato
                flow_data['data_completa'] = pd.to_datetime(
                    flow_data['anno'].astype(str) + '-' + 
                    flow_data['mese'].astype(str) + '-01'
                )
                flow_data = flow_data[
                    (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
                    (flow_data['data_completa'] <= pd.Timestamp(end_date))
                ]
                
                # Calcola metriche flusso
                total_flow = flow_data['flusso_mensile'].sum()
                avg_monthly_flow = flow_data.groupby(['anno', 'mese'])['flusso_mensile'].sum().mean()
                num_months = flow_data[['anno', 'mese']].drop_duplicates().shape[0]
                max_flow = flow_data['flusso_mensile'].max()
                min_flow = flow_data['flusso_mensile'].min()
                
                # Calcola metriche stock (dati originali)
                # Prendi l'ultimo mese disponibile nel periodo
                filtered_data['data_completa'] = pd.to_datetime(filtered_data['data_riferimento'])
                filtered_data = filtered_data.sort_values('data_completa')
                last_date = filtered_data['data_completa'].max()
                last_month_data = filtered_data[filtered_data['data_completa'] == last_date]
                total_stock = last_month_data[value_column].sum()
                
                # Display metriche in tabs
                tab_flow, tab_stock = st.tabs(["Metriche Flussi", "Metriche Stock"])
                
                with tab_flow:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            label="Flusso totale nel periodo",
                            value=f"{total_flow:,.0f}",
                            help=f"Somma dei flussi netti da {start_date.strftime('%b %Y')} a {end_date.strftime('%b %Y')}"
                        )
                    
                    with col2:
                        st.metric(
                            label="Numero di mesi",
                            value=f"{num_months}",
                            help="Mesi considerati nell'intervallo selezionato"
                        )
                    
                    with col3:
                        st.metric(
                            label="Flusso mensile medio",
                            value=f"{avg_monthly_flow:,.0f}",
                            help="Media dei flussi mensili nel periodo"
                        )
                
                with tab_stock:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            label=f"Stock cumulativo al {last_date.strftime('%b %Y')}",
                            value=f"{total_stock:,.0f}",
                            help="Valore cumulativo originale all'ultimo mese del periodo"
                        )
                    
                    with col2:
                        # Calcola variazione percentuale rispetto al primo mese del periodo
                        first_date = filtered_data['data_completa'].min()
                        first_month_data = filtered_data[filtered_data['data_completa'] == first_date]
                        first_stock = first_month_data[value_column].sum()
                        
                        if first_stock > 0:
                            pct_change = ((total_stock - first_stock) / first_stock) * 100
                        else:
                            pct_change = 0
                        
                        st.metric(
                            label="Variazione nel periodo (selezionare 2 mesi nello stesso anno)",
                            value=f"{pct_change:+.1f}%",
                            help=f"Variazione percentuale da {first_date.strftime('%b %Y')} a {last_date.strftime('%b %Y')}"
                        )
                    
                    with col3:
                        # Media stock mensile
                        avg_stock = filtered_data.groupby(['data_completa'])[value_column].sum().mean()
                        st.metric(
                            label="Stock mensile medio",
                            value=f"{avg_stock:,.0f}",
                            help="Media dei valori cumulativi nei mesi del periodo"
                        )
                
                # Warning per valori negativi
                negative_flows = flow_data[flow_data['flusso_mensile'] < 0]
                if not negative_flows.empty:
                    st.warning(f"""
                    **Attenzione:** Sono presenti {len(negative_flows)} valori di flusso negativo nel periodo selezionato.  
                    Questo può essere dovuto a:  
                    - Correzioni retroattive nei dati originali (consolidamento)
                    - Diminuzioni effettive del numero di migranti
                    - Errori nel processo di estrazione dei dati
                    """)
        
        elif is_sbarchi:
            # Metriche per dati_sbarchi (non modificato)
            value_column = 'migranti_sbarcati'
            total_sbarchi = filtered_data[value_column].sum()
            avg_daily = filtered_data[value_column].mean()
            max_daily = filtered_data[value_column].max()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    label="Totale sbarchi nel periodo",
                    value=f"{total_sbarchi:,.0f}",
                    help=f"Totale sbarchi da {start_date} a {end_date}"
                )
            
            with col2:
                st.metric(
                    label="Media giornaliera",
                    value=f"{avg_daily:,.1f}",
                    help="Media di migranti sbarcati al giorno"
                )
            
            with col3:
                st.metric(
                    label="Massimo giornaliero",
                    value=f"{max_daily:,.0f}",
                    help="Numero massimo di migranti sbarcati in un singolo giorno"
                )
        
        # Visualizzazioni specifiche per dataset
        st.header("Analisi Dettagliata")
        
        if selected_table == 'dati_nazionalita':
            # Aggiungi toggle per flussi/stock
            col_toggle, _ = st.columns([1, 3])
            with col_toggle:
                view_mode_naz = st.radio(
                    "Modalità di visualizzazione:",
                    ["Flussi mensili (calcolati)", "Dati cumulativi originali (selezionare 1 solo mese)"],
                    horizontal=True,
                    key="view_mode_naz"
                )
            
            # Layout a due colonne per nazionalità
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if view_mode_naz == "Flussi mensili (calcolati)":
                    st.subheader("Andamento temporale per nazionalità (flusso)")
                    selected_nazionalita = st.session_state.get('selected_nazionalita', [])
                    if selected_nazionalita:
                        fig_trend = create_nationality_trend_chart(
                            filtered_data, 
                            selected_nazionalita,
                            start_date,
                            end_date
                        )
                        if fig_trend:
                            st.plotly_chart(fig_trend, use_container_width=True)
                        else:
                            st.info("Nessun dato disponibile per le nazionalità selezionate nel periodo scelto.")
                    else:
                        st.info("Seleziona almeno una nazionalità per visualizzare il grafico.")
                else:
                    st.subheader("Andamento temporale per nazionalità (stock)")
                    selected_nazionalita = st.session_state.get('selected_nazionalita', [])
                    if selected_nazionalita:
                        fig_trend_stock = create_nationality_stock_trend_chart(
                            filtered_data, 
                            selected_nazionalita,
                            start_date,
                            end_date
                        )
                        if fig_trend_stock:
                            st.plotly_chart(fig_trend_stock, use_container_width=True)
                        else:
                            st.info("Nessun dato disponibile per le nazionalità selezionate nel periodo scelto.")
                    else:
                        st.info("Seleziona almeno una nazionalità per visualizzare il grafico.")
            
            with col2:
                if view_mode_naz == "Flussi mensili (calcolati)":
                    st.subheader("Distribuzione flussi per nazionalità")
                    selected_nazionalita = st.session_state.get('selected_nazionalita', [])
                    if selected_nazionalita:
                        fig_bar = create_nationality_bar_chart(
                            filtered_data,
                            start_date,
                            end_date,
                            selected_nazionalita
                        )
                        if fig_bar:
                            st.plotly_chart(fig_bar, use_container_width=True)
                        else:
                            st.info("Nessun dato disponibile per le nazionalità selezionate nel periodo scelto.")
                    else:
                        st.info("Seleziona almeno una nazionalità per visualizzare il grafico.")
                else:
                    st.subheader("Distribuzione stock per nazionalità")
                    selected_nazionalita = st.session_state.get('selected_nazionalita', [])
                    if selected_nazionalita:
                        fig_bar_stock = create_nationality_stock_bar_chart(
                            filtered_data,
                            start_date,
                            end_date,
                            selected_nazionalita
                        )
                        if fig_bar_stock:
                            st.plotly_chart(fig_bar_stock, use_container_width=True)
                        else:
                            st.info("Nessun dato disponibile per le nazionalità selezionate nel periodo scelto.")
                    else:
                        st.info("Seleziona almeno una nazionalità per visualizzare il grafico.")
        
        elif selected_table == 'dati_accoglienza':
            # Aggiungi toggle per flussi/stock
            col_toggle, _ = st.columns([1, 3])
            with col_toggle:
                view_mode_acc = st.radio(
                    "Modalità di visualizzazione:",
                    ["Flussi mensili (calcolati)", "Dati cumulativi originali (selezionare 1 solo mese)"],
                    horizontal=True,
                    key="view_mode_acc"
                )
            
            # Layout a due colonne per accoglienza
            col1, col2 = st.columns(2)
            
            with col1:
                if view_mode_acc == "Flussi mensili (calcolati)":
                    st.subheader("Distribuzione regionale (flusso)")
                    selected_tipologie = st.session_state.get('selected_tipologie', [])
                    fig_map = create_regional_flow_map(
                        filtered_data,
                        selected_tipologie,
                        start_date,
                        end_date
                    )
                else:
                    st.subheader("Distribuzione regionale (stock)")
                    selected_tipologie = st.session_state.get('selected_tipologie', [])
                    fig_map = create_regional_stock_map(
                        filtered_data,
                        selected_tipologie,
                        start_date,
                        end_date
                    )
                
                if fig_map:
                    st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.info("Nessun dato disponibile per le regioni selezionate nel periodo scelto.")
            
            with col2:
                if view_mode_acc == "Flussi mensili (calcolati)":
                    st.subheader("Tipologie di accoglienza (flusso)")
                    selected_tipologie = st.session_state.get('selected_tipologie', [])
                    fig_pie = create_accommodation_pie_chart(
                        filtered_data,
                        selected_tipologie,
                        start_date,
                        end_date
                    )
                else:
                    st.subheader("Tipologie di accoglienza (stock)")
                    selected_tipologie = st.session_state.get('selected_tipologie', [])
                    fig_pie = create_accommodation_stock_pie_chart(
                        filtered_data,
                        selected_tipologie,
                        start_date,
                        end_date
                    )
                
                if fig_pie:
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info("Nessun dato disponibile per le tipologie selezionate nel periodo scelto.")
            
            # Tabella riepilogativa flussi mensili
            with st.expander("Tabella riepilogativa flussi mensili"):
                if 'selected_regioni' in st.session_state and st.session_state.selected_regioni:
                    # Calcola flussi per regione
                    flow_data = calculate_monthly_flow(
                        filtered_data,
                        group_columns=['regione'],
                        value_column='totale_accoglienza'
                    )
                    
                    if not flow_data.empty:
                        flow_data['data_completa'] = pd.to_datetime(
                            flow_data['anno'].astype(str) + '-' + 
                            flow_data['mese'].astype(str) + '-01'
                        )
                        flow_data = flow_data[
                            (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
                            (flow_data['data_completa'] <= pd.Timestamp(end_date))
                        ]
                        
                        # Pivot table per visualizzazione
                        pivot_table = flow_data.pivot_table(
                            values='flusso_mensile',
                            index='regione',
                            columns=['anno', 'mese'],
                            aggfunc='sum',
                            fill_value=0
                        )
                        
                        # Riformatta i nomi delle colonne
                        pivot_table.columns = [f"{anno}-{mese:02d}" for anno, mese in pivot_table.columns]
                        pivot_table = pivot_table.round(0)
                        
                        st.dataframe(pivot_table, use_container_width=True)
                        
                        # Opzione download
                        csv = pivot_table.to_csv()
                        st.download_button(
                            label="Scarica CSV flussi mensili",
                            data=csv,
                            file_name=f"flussi_accoglienza_{start_date}_{end_date}.csv",
                            mime="text/csv"
                        )
        
        elif selected_table == 'dati_sbarchi':
            # Layout per dati_sbarchi (NON MODIFICATO)
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
            
            # Sezione dati grezzi per dati_sbarchi
            with st.expander("Dati Grezzi"):
                if daily_data is not None:
                    display_data = daily_data.sort_values('data_completa').copy()
                    display_data = display_data.rename(columns={
                        'data_completa': 'Data',
                        'migranti_sbarcati': 'Migranti Sbarcati'
                    })
                    st.dataframe(display_data, use_container_width=True)
                    
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
                # Tabs per dati originali e flussi calcolati
                tab1, tab2 = st.tabs(["Dati originali (stock)", "Flussi calcolati"])
                
                with tab1:
                    st.markdown("**Dati cumulativi originali dal Ministero**")
                    st.dataframe(filtered_data, use_container_width=True)
                    
                    csv_original = filtered_data.to_csv(index=False)
                    st.download_button(
                        label="Scarica CSV dati originali",
                        data=csv_original,
                        file_name=f"{selected_table}_originali_{start_date}_{end_date}.csv",
                        mime="text/csv"
                    )
                
                with tab2:
                    st.markdown("**Flussi mensili calcolati**")
                    
                    # Calcola e mostra flussi
                    group_columns = ['nazionalita'] if selected_table == 'dati_nazionalita' else ['regione']
                    value_column = 'migranti_sbarcati' if selected_table == 'dati_nazionalita' else 'totale_accoglienza'
                    
                    flow_data = calculate_monthly_flow(
                        filtered_data,
                        group_columns=group_columns,
                        value_column=value_column
                    )
                    
                    if not flow_data.empty:
                        # Filtra per periodo
                        flow_data['data_completa'] = pd.to_datetime(
                            flow_data['anno'].astype(str) + '-' + 
                            flow_data['mese'].astype(str) + '-01'
                        )
                        flow_data = flow_data[
                            (flow_data['data_completa'] >= pd.Timestamp(start_date)) & 
                            (flow_data['data_completa'] <= pd.Timestamp(end_date))
                        ]
                        
                        # Formatta per visualizzazione
                        display_flow = flow_data[[
                            'anno', 'mese', 
                            group_columns[0], 
                            'valore_ffill', 
                            'flusso_mensile'
                        ]].copy()
                        
                        display_flow = display_flow.rename(columns={
                            group_columns[0]: group_columns[0].capitalize(),
                            'valore_ffill': 'Valore cumulativo',
                            'flusso_mensile': 'Flusso mensile'
                        })
                        
                        st.dataframe(display_flow, use_container_width=True)
                        
                        csv_flow = display_flow.to_csv(index=False)
                        st.download_button(
                            label="Scarica CSV flussi calcolati",
                            data=csv_flow,
                            file_name=f"{selected_table}_flussi_{start_date}_{end_date}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("Nessun dato di flusso disponibile per il periodo selezionato.")
    
    else:
        st.warning("Nessun dato disponibile per i filtri selezionati")
        
except Exception as e:
    st.error(f"Errore nell'elaborazione dei datos: {str(e)}")
    st.info("Controlla i log di Streamlit Cloud per maggiori dettagli")

# Footer informativo aggiornato
st.markdown("---")
ultima_data, ultimo_file = get_ultimo_aggiornamento()
st.markdown(
    f"""
    **INFO E METODOLOGIA**
    
    **Fonte dati:** Cruscotto statistico del Ministero dell'Interno (2017-2025)  
    **URL:** https://libertaciviliimmigrazione.dlci.interno.gov.it/documentazione/dati-e-statistiche/cruscotto-statistico-giornaliero  
    **Ultimo aggiornamento:** "{ultimo_file}"  
    **Repository GitHub:** [MDA_2025_progetto_tesi](https://github.com/paoloRi/MDA_2025_progetto_tesi)
    
    **METODOLOGIA DI ANALISI PER DATI CUMULATIVI (NAZIONALITÀ E ACCOGLIENZA)**  
    I dati originali sono cumulativi annuali. Per analizzare i flussi mensili sono stati applicati le seguenti metodologie:  
    1. **Calcolo flusso mensile:** valore del mese corrente - valore del mese precedente  
    2. **Gestione mesi mancanti:** utilizzato l'ultimo dato disponibile (forward fill)
    
    **LIMITI DELLA TRASFORMAZIONE**  
    I flussi calcolati potrebbero non coincidere con i flussi effettivi a causa di:  
    - Rettifiche retroattive nei dati originali (consolidamento)
    - Errori nei dati originali  
    - Errori nel processo di estrazione dei dati
    
    **NOTE SUI DATI**  
    - **Dati relativi a nazionalità e migranti in accoglienza:** cumulativi annuali, trasformati in flussi mensili  
    - **Dati relativi al numero di migranti sbarcati:** flussi giornalieri (dati originali)  
    
    """
)
