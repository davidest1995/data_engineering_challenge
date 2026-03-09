"""
DASHBOARD STREAMLIT - GLOBANT CHALLENGE
Visualización interactiva con identidad visual Globant
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyodbc
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

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

# ==================== FUNCIONES ====================
def get_db_connection():
    """Conectar a la BD"""
    db_server = os.getenv('DB_SERVER')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password};TrustServerCertificate=yes'
    conn = pyodbc.connect(conn_str)
    return conn

@st.cache_data(ttl=5)
def load_data():
    """Cargar todos los datos de la BD"""
    conn = get_db_connection()
    employees = pd.read_sql_query("SELECT * FROM hired_employees", conn)
    departments = pd.read_sql_query("SELECT * FROM departments", conn)
    jobs = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()
    
    # Convertir datetime y crear columnas auxiliares
    employees['datetime'] = pd.to_datetime(employees['datetime'])
    employees['year'] = employees['datetime'].dt.year
    employees['month'] = employees['datetime'].dt.month
    
    def get_quarter(month):
        if month in [1,2,3]: return 'Q1'
        elif month in [4,5,6]: return 'Q2'
        elif month in [7,8,9]: return 'Q3'
        else: return 'Q4'
    
    employees['quarter'] = employees['month'].apply(get_quarter)
    
    return employees, departments, jobs

def get_employees_by_quarter(employees, departments, jobs, year):
    """Calcular empleados por trimestre para un año específico"""
    employees_year = employees[employees['year'] == year].copy()
    if employees_year.empty:
        return pd.DataFrame(columns=['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4'])
    
    merged = employees_year.merge(departments, left_on='department_id', right_on='id')
    merged = merged.merge(jobs, left_on='job_id', right_on='id')
    
    result = merged.groupby(['department', 'job', 'quarter']).size().reset_index(name='count')
    pivot = result.pivot_table(index=['department','job'], columns='quarter', values='count', fill_value=0).reset_index()
    
    for q in ['Q1','Q2','Q3','Q4']:
        if q not in pivot.columns:
            pivot[q] = 0
    
    pivot = pivot[['department','job','Q1','Q2','Q3','Q4']].sort_values(['department','job'])
    return pivot

def get_departments_above_mean(employees, departments, year):
    """Calcular departamentos sobre el promedio para un año específico"""
    employees_year = employees[employees['year'] == year].copy()
    if employees_year.empty:
        return pd.DataFrame(columns=['department', 'hired']), 0
    
    dept_counts = employees_year.groupby('department_id').size().reset_index(name='hired')
    dept_counts = dept_counts.merge(departments, left_on='department_id', right_on='id')
    mean = dept_counts['hired'].mean()
    above_mean = dept_counts[dept_counts['hired'] > mean].sort_values('hired', ascending=False)
    return above_mean, mean

# ==================== HEADER CON LOGO ====================
st.markdown("""
    <div class="globant-header">
        <div class="globant-logo">GLOBANT <span>CHALLENGE</span></div>
    </div>
""", unsafe_allow_html=True)

# ==================== MAIN ====================
def main():
    # Cargar datos
    try:
        employees, departments, jobs = load_data()
    except Exception as e:
        st.error(f"❌ Error al cargar datos: {e}. Asegúrate de cargar los CSV primero.")
        return
    
    available_years = sorted(employees['year'].unique())
    if not available_years:
        st.warning("No hay datos de empleados disponibles.")
        return
    
    # ==================== SIDEBAR ====================
    with st.sidebar:
        st.markdown('<div class="sidebar-header">🎯 FILTROS</div>', unsafe_allow_html=True)
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
    employees_year = employees[employees['year'] == selected_year]
    
    st.markdown("## 📈 MÉTRICAS CLAVE")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("👥 Total Empleados (histórico)", len(employees))
    with col2:
        st.metric("🏢 Departamentos", len(departments))
    with col3:
        st.metric("💼 Trabajos", len(jobs))
    with col4:
        st.metric(f"📅 Contrataciones {selected_year}", len(employees_year))
    
    st.divider()
    
    # ==================== ANÁLISIS 1: POR TRIMESTRE ====================
    st.markdown(f"## 📊 ANÁLISIS 1: Empleados por Trimestre ({selected_year})")
    st.write("Distribución de contrataciones por trimestre, departamento y trabajo")
    
    quarter_data = get_employees_by_quarter(employees, departments, jobs, selected_year)
    
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
    
    above_mean, mean_val = get_departments_above_mean(employees, departments, selected_year)
    
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
    st.markdown(f"## 📊 ANÁLISIS ADICIONAL ({selected_year})")
    st.subheader("🥇 Top 10 Trabajos más Contratados")
    
    if employees_year.empty:
        st.info(f"No hay contrataciones en {selected_year}.")
    else:
        top_jobs = employees_year.merge(jobs, left_on='job_id', right_on='id')\
            .groupby('job').size().reset_index(name='count')\
            .sort_values('count', ascending=False).head(10)
        
        fig = px.bar(
            top_jobs,
            x='count',
            y='job',
            orientation='h',
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