"""
DASHBOARD STREAMLIT - GLOBANT CHALLENGE
Visualización interactiva con identidad visual Globant
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import os

# ==================== CONFIGURACIÓN ====================
st.set_page_config(
    page_title="Globant Challenge Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== IDENTIDAD VISUAL GLOBANT ====================
COLORES_GLOBANT = {
    'azul_principal': '#1A4CFF',   # Globant Blue - color más representativo
    'azul_oscuro':    '#0D1B3A',   # Dark Blue - fondos y textos
    'fondo':          '#F5F7FA',   # Background gris claro
    'blanco':         '#FFFFFF',   # Tarjetas y contenedores
    'teal':           '#00C1B6',   # Acento verde-azulado
    'coral':          '#FF6B6B',   # Acento coral / alertas
    'gris_claro':     '#E8ECF2',   # Bordes y separadores
    'gris_oscuro':    '#4A5568',   # Texto secundario
    'texto':          '#0D1B3A'    # Texto principal
}

# Paleta para gráficos (secuencia discreta)
PALETA_GLOBANT = [
    COLORES_GLOBANT['azul_principal'],
    COLORES_GLOBANT['teal'],
    COLORES_GLOBANT['coral'],
    '#5B8AF5',   # Azul medio
    '#2DD4BF',   # Teal claro
    '#FFA07A',   # Salmón
    COLORES_GLOBANT['azul_oscuro']
]

# CSS personalizado con colores Globant
st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        html, body, [class*="css"] {{
            font-family: 'Inter', sans-serif;
        }}
        .main {{
            padding: 2rem;
            background-color: {COLORES_GLOBANT['fondo']};
        }}
        .block-container {{
            background-color: {COLORES_GLOBANT['fondo']};
        }}
        .metric-card {{
            background-color: {COLORES_GLOBANT['blanco']};
            padding: 1.5rem;
            border-radius: 0.75rem;
            margin: 0.5rem 0;
            border-left: 5px solid {COLORES_GLOBANT['azul_principal']};
            box-shadow: 0 2px 8px rgba(26,76,255,0.08);
        }}
        h1 {{
            color: {COLORES_GLOBANT['azul_oscuro']};
            text-align: center;
            margin-bottom: 1rem;
            font-weight: 800;
        }}
        h2 {{
            color: {COLORES_GLOBANT['azul_principal']};
            margin-top: 2rem;
            border-bottom: 3px solid {COLORES_GLOBANT['azul_principal']};
            padding-bottom: 0.5rem;
            font-weight: 700;
        }}
        h3 {{
            color: {COLORES_GLOBANT['azul_oscuro']};
            font-weight: 600;
        }}
        .globant-header {{
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 1rem;
        }}
        .globant-logo {{
            font-size: 2.5rem;
            font-weight: 800;
            color: {COLORES_GLOBANT['azul_oscuro']};
            letter-spacing: 2px;
        }}
        .globant-logo span {{
            color: {COLORES_GLOBANT['azul_principal']};
        }}
        .sidebar-header {{
            font-size: 1.5rem;
            font-weight: 700;
            color: {COLORES_GLOBANT['azul_oscuro']};
            text-align: center;
            margin-bottom: 1rem;
        }}
        .stButton>button {{
            background-color: {COLORES_GLOBANT['azul_principal']};
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 1.25rem;
            font-weight: 600;
            transition: background-color 0.2s ease;
        }}
        .stButton>button:hover {{
            background-color: {COLORES_GLOBANT['azul_oscuro']};
            color: white;
        }}
        .stMetric {{
            background-color: {COLORES_GLOBANT['blanco']};
            padding: 1rem;
            border-radius: 0.75rem;
            border-left: 5px solid {COLORES_GLOBANT['teal']};
            box-shadow: 0 2px 8px rgba(0,193,182,0.1);
        }}
        footer {{
            text-align: center;
            color: {COLORES_GLOBANT['gris_oscuro']};
            font-size: 0.9rem;
            margin-top: 3rem;
            border-top: 2px solid {COLORES_GLOBANT['gris_claro']};
            padding-top: 1rem;
        }}
        /* Divider */
        hr {{
            border-color: {COLORES_GLOBANT['gris_claro']};
        }}
    </style>
""", unsafe_allow_html=True)

# ==================== CONFIGURACIÓN API ====================
API_BASE_URL = os.getenv('API_BASE_URL', 'http://globant-api:5001')


# ==================== FUNCIONES DE DATOS (vía API REST) ====================

@st.cache_data(ttl=30)
def fetch_status():
    """Obtener estado general de tablas desde la API"""
    resp = requests.get(f"{API_BASE_URL}/api/status", timeout=10)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=30)
def fetch_years():
    """Obtener lista de años disponibles desde la API"""
    resp = requests.get(f"{API_BASE_URL}/api/analytics/years", timeout=10)
    resp.raise_for_status()
    return resp.json()['years']


@st.cache_data(ttl=30)
def fetch_employees_by_quarter(year: int = 2021):
    """Obtener empleados por trimestre desde la API"""
    resp = requests.get(f"{API_BASE_URL}/api/analytics/employees-by-quarter", params={'year': year}, timeout=10)
    resp.raise_for_status()
    data = resp.json()['data']
    return pd.DataFrame(data)


@st.cache_data(ttl=30)
def fetch_departments_above_mean(year: int = 2021):
    """Obtener departamentos sobre el promedio desde la API"""
    resp = requests.get(f"{API_BASE_URL}/api/analytics/departments-above-mean", params={'year': year}, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    return pd.DataFrame(result['data']), result['mean_hired']


@st.cache_data(ttl=30)
def fetch_top_jobs(year: int = 2021):
    """Obtener top 10 trabajos desde la API"""
    resp = requests.get(f"{API_BASE_URL}/api/analytics/top-jobs", params={'year': year}, timeout=10)
    resp.raise_for_status()
    return pd.DataFrame(resp.json()['data'])

# ==================== HEADER CON LOGO ====================
st.markdown("""
    <div class="globant-header">
        <div class="globant-logo">GLOBANT <span>CHALLENGE</span></div>
    </div>
""", unsafe_allow_html=True)

# ==================== MAIN ====================
def main():
    # ==================== CARGAR STATUS ====================
    try:
        status = fetch_status()
        tables = status.get('tables', {})
    except Exception as e:
        st.error(f"❌ No se puede conectar a la API: {e}. Verifica que el servicio esté activo.")
        return

    total_employees = tables.get('hired_employees', 0)
    total_depts     = tables.get('departments', 0)
    total_jobs      = tables.get('jobs', 0)

    if total_employees == 0:
        st.warning("No hay datos de empleados. Carga los CSV primero.")
        return

    # ==================== SIDEBAR ====================
    try:
        available_years = fetch_years()
    except Exception:
        available_years = [2021]

    with st.sidebar:
        st.markdown('<div class="sidebar-header">🎯 ANÁLISIS</div>', unsafe_allow_html=True)
        
        selected_year = st.selectbox(
            "Selecciona el año de análisis:",
            options=available_years,
            index=available_years.index(2021) if 2021 in available_years else 0
        )
        
        st.markdown("---")
        st.markdown(
            f"""
            <div style='text-align: center; color: {COLORES_GLOBANT["gris_oscuro"]};'>
                <b>Globant</b><br>
                <span style='font-size: 0.9rem;'>Digital Native</span>
            </div>
            """,
            unsafe_allow_html=True
        )

    # ==================== MÉTRICAS CLAVE ====================
    st.markdown("## 📈 MÉTRICAS CLAVE")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("👥 Total Empleados", total_employees)
    with col2:
        st.metric("🏢 Departamentos", total_depts)
    with col3:
        st.metric("💼 Trabajos", total_jobs)

    st.divider()
    
    # ==================== ANÁLISIS 1: POR TRIMESTRE ====================
    st.markdown(f"## 📈 ANÁLISIS 1: Empleados por Trimestre ({selected_year})")
    st.write("Distribución de contrataciones por trimestre, departamento y trabajo")

    try:
        quarter_data = fetch_employees_by_quarter(selected_year)
    except Exception as e:
        st.error(f"❌ Error al cargar Análisis 1: {e}")
        quarter_data = pd.DataFrame()
    
    if quarter_data.empty:
        st.info(f"No hay datos de contrataciones para el año {selected_year}.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            depts = ["Todos"] + sorted(quarter_data['department'].unique().tolist())
            selected_dept = st.selectbox("Departamento (opcional):", depts)
        with col2:
            jobs_list = ["Todos"] + sorted(quarter_data['job'].unique().tolist())
            selected_job = st.selectbox("Trabajo (opcional):", jobs_list)
        
        filtered = quarter_data.copy()
        if selected_dept != "Todos":
            filtered = filtered[filtered['department'] == selected_dept]
        if selected_job != "Todos":
            filtered = filtered[filtered['job'] == selected_job]
        
        # Preparar datos para gráfico
        melted = filtered.melt(id_vars=['department','job'], value_vars=['Q1','Q2','Q3','Q4'],
                               var_name='Quarter', value_name='Employees')
        
        # Agregación para que plotly no superponga barras
        if selected_dept != "Todos" and selected_job == "Todos":
            # Filtrado por departamento, mostrar por trabajo
            agg_data = melted.groupby(['Quarter', 'job'])['Employees'].sum().reset_index()
            color_col = 'job'
            title = f"Empleados por Trimestre - Depto: {selected_dept}"
        elif selected_job != "Todos" and selected_dept == "Todos":
            # Filtrado por trabajo, mostrar por departamento
            agg_data = melted.groupby(['Quarter', 'department'])['Employees'].sum().reset_index()
            color_col = 'department'
            title = f"Empleados por Trimestre - Trabajo: {selected_job}"
        elif selected_dept != "Todos" and selected_job != "Todos":
            # Ambos filtros
            agg_data = melted.copy()
            agg_data['Group'] = agg_data['department'] + " - " + agg_data['job']
            color_col = 'Group'
            title = f"Empleados por Trimestre - {selected_dept} & {selected_job}"
        else:
            # Todos los datos, agrupado por departamento
            agg_data = melted.groupby(['Quarter', 'department'])['Employees'].sum().reset_index()
            color_col = 'department'
            title = f"Empleados por Trimestre ({selected_year})"
        
        # Expandir paleta por si hay muchos grupos
        num_colors = len(agg_data[color_col].unique())
        paleta_extendida = (PALETA_GLOBANT * ((num_colors // len(PALETA_GLOBANT)) + 1))[:max(num_colors, 1)]
        
        fig = px.bar(
            agg_data,
            x='Quarter',
            y='Employees',
            color=color_col,
            barmode='group',
            title=title,
            labels={'Employees': 'Número de Empleados', 'Quarter': 'Trimestre', color_col: 'Agrupación'},
            color_discrete_sequence=paleta_extendida,
            template='plotly_white',
            height=400
        )
        fig.update_layout(
            hovermode='x unified',
            font=dict(size=12, family="Arial"),
            title_font_color=COLORES_GLOBANT['azul_oscuro'],
            legend_title_font_color=COLORES_GLOBANT['azul_oscuro']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # ==================== ANÁLISIS 2: SOBRE PROMEDIO ====================
    st.markdown(f"## 🏆 ANÁLISIS 2: Departamentos sobre el Promedio ({selected_year})")

    try:
        above_mean, mean_val = fetch_departments_above_mean(selected_year)
    except Exception as e:
        st.error(f"❌ Error al cargar Análisis 2: {e}")
        above_mean, mean_val = pd.DataFrame(), 0
    
    if above_mean.empty:
        st.info(f"No hay suficientes datos para el año {selected_year}.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Promedio de Contrataciones", f"{mean_val:.1f}")
        col2.metric("🔝 Departamentos sobre Promedio", len(above_mean))
        col3.metric("📈 Máximas Contrataciones", above_mean['hired'].max())
        
        # Calcular aumento porcentual frente al promedio
        above_mean = above_mean.copy()
        above_mean['pct_vs_mean'] = ((above_mean['hired'] - mean_val) / mean_val * 100).round(1)
        above_mean['text_label'] = above_mean['pct_vs_mean'].apply(lambda x: f"+{x:.1f}%")

        fig = px.bar(
            above_mean,
            x='department',
            y='hired',
            color='hired',
            text='text_label',
            custom_data=['pct_vs_mean'],
            title=f'Departamentos que Contrataron sobre el Promedio ({selected_year})',
            labels={'hired': 'Número de Empleados', 'department': 'Departamento'},
            color_continuous_scale=[[0, COLORES_GLOBANT['teal']], [1, COLORES_GLOBANT['azul_principal']]],
            template='plotly_white',
            height=400
        )
        fig.update_traces(
            textposition='outside',
            textfont=dict(size=11, color=COLORES_GLOBANT['azul_oscuro']),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Contrataciones: %{y}<br>"
                "Aumento vs promedio: <b>+%{customdata[0]:.1f}%</b>"
                "<extra></extra>"
            )
        )
        fig.add_hline(y=mean_val, line_dash="dash", line_color=COLORES_GLOBANT['azul_oscuro'],
                      annotation_text=f"Promedio: {mean_val:.1f}", annotation_position="right")
        fig.update_layout(
            hovermode='x',
            font=dict(size=12),
            xaxis_tickangle=-45,
            title_font_color=COLORES_GLOBANT['azul_oscuro'],
            uniformtext_minsize=9,
            uniformtext_mode='hide'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # ==================== ANÁLISIS ADICIONAL ====================
    st.markdown(f"## 📈 ANÁLISIS ADICIONAL ({selected_year})")
    st.subheader("🥇 Top 10 Trabajos más Contratados")

    try:
        top_jobs_df = fetch_top_jobs(selected_year)
    except Exception as e:
        st.error(f"❌ Error al cargar Top Jobs: {e}")
        top_jobs_df = pd.DataFrame()

    if top_jobs_df.empty:
        st.info(f"No hay contrataciones en {selected_year}.")
    else:
        fig = px.bar(
            top_jobs_df, x='count', y='job', orientation='h',
            title=f'Top 10 Trabajos ({selected_year})',
            labels={'count': 'Contrataciones', 'job': 'Trabajo'},
            color='count',
            color_continuous_scale=[[0, COLORES_GLOBANT['teal']], [1, COLORES_GLOBANT['azul_oscuro']]],
            template='plotly_white',
            height=400
        )
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            title_font_color=COLORES_GLOBANT['azul_oscuro']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Footer con identidad Globant
    st.markdown(f"""
        <footer>
            <b style='color: {COLORES_GLOBANT["azul_oscuro"]}'>Globant</b> Challenge - Dashboard Interactivo<br>
            <span style='color: {COLORES_GLOBANT["azul_principal"]}'>#BeGlobant</span> | Data driven decisions
        </footer>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()