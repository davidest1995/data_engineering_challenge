"""
API REST para Migración de Datos - Desafío Globant
Maneja: Carga de CSV, Inserción en BD, Backup/Restore, Análisis
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import pyodbc
import os
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import logging
from functools import wraps

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        #local
        #logging.FileHandler('../logs/app.log'),
        #docker
        logging.FileHandler('/app/logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuración
#local
#DB_PATH = '../database/challenge.db'
#BACKUP_DIR = '../backups'
#ERROR_LOG_FILE = '../logs/data_errors.log'
#docker
DB_SERVER = os.getenv('DB_SERVER', 'sqlserver')
DB_NAME = os.getenv('DB_NAME', 'challenge')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Your_Password123!')

BACKUP_DIR = '/app/backups'
ERROR_LOG_FILE = '/app/logs/data_errors.log'




MAX_BATCH_SIZE = 1000

# Crear directorio de backups
os.makedirs(BACKUP_DIR, exist_ok=True)


# ==================== UTILIDADES ====================

def get_db_connection(use_database=True):
    """Obtener conexión a la BD"""
    db = DB_NAME if use_database else 'master'
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_SERVER};DATABASE={db};UID={DB_USER};PWD={DB_PASSWORD};TrustServerCertificate=yes'
    conn = pyodbc.connect(conn_str, autocommit=not use_database)
    return conn

def init_database():
    """Inicializar la base de datos con las tablas"""
    try:
        conn = get_db_connection(use_database=False)
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{DB_NAME}') BEGIN CREATE DATABASE {DB_NAME} END")
        conn.close()
    except Exception as e:
        logger.warning(f"No se pudo crear la base de datos (quizá ya existe o no hay acceso master): {e}")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Tabla de Departamentos
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments' and xtype='U')
        CREATE TABLE departments (
            id INT PRIMARY KEY,
            department NVARCHAR(255) NOT NULL UNIQUE
        )
    ''')
    
    # Tabla de Trabajos
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='jobs' and xtype='U')
        CREATE TABLE jobs (
            id INT PRIMARY KEY,
            job NVARCHAR(255) NOT NULL UNIQUE
        )
    ''')
    
    # Tabla de Empleados Contratados
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hired_employees' and xtype='U')
        CREATE TABLE hired_employees (
            id INT PRIMARY KEY,
            name NVARCHAR(255) NOT NULL,
            datetime NVARCHAR(255) NOT NULL,
            department_id INT NOT NULL,
            job_id INT NOT NULL,
            FOREIGN KEY (department_id) REFERENCES departments(id),
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada correctamente")


def log_error(table: str, row_num: int, reason: str, data: dict):
    """Registrar errores de validación"""
    error_msg = f"[{datetime.now()}] Tabla: {table} | Fila: {row_num} | Razón: {reason} | Datos: {data}"
    logger.error(error_msg)
    with open(ERROR_LOG_FILE, 'a') as f:
        f.write(error_msg + '\n')


def validate_employee(data: dict, row_num: int) -> bool:
    """Validar registro de empleado"""
    import math
    
    required_fields = ['id', 'name', 'datetime', 'department_id', 'job_id']
    
    # Verificar que todos los campos estén presentes y no vacíos
    for field in required_fields:
        value = data.get(field)
        
        # Detectar None, strings vacíos, y NaN (de pandas)
        is_empty = (
            value is None or 
            (isinstance(value, float) and math.isnan(value)) or
            (isinstance(value, str) and value.strip() == '')
        )
        
        if is_empty:
            log_error('hired_employees', row_num, f'Campo requerido faltante: {field}', data)
            return False
    
    # Validar que IDs sean números
    try:
        int(data['id'])
        int(data['department_id'])
        int(data['job_id'])
    except (ValueError, TypeError):
        log_error('hired_employees', row_num, 'ID debe ser número entero', data)
        return False
    
    # Validar formato datetime ISO
    try:
        datetime.fromisoformat(data['datetime'].replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        log_error('hired_employees', row_num, 'datetime debe estar en formato ISO', data)
        return False
    
    return True


def validate_department(data: dict, row_num: int) -> bool:
    """Validar registro de departamento"""
    required_fields = ['id', 'department']
    
    for field in required_fields:
        if field not in data or data[field] is None or str(data[field]).strip() == '':
            log_error('departments', row_num, f'Campo requerido faltante: {field}', data)
            return False
    
    try:
        int(data['id'])
    except (ValueError, TypeError):
        log_error('departments', row_num, 'ID debe ser número entero', data)
        return False
    
    return True


def validate_job(data: dict, row_num: int) -> bool:
    """Validar registro de trabajo"""
    required_fields = ['id', 'job']
    
    for field in required_fields:
        if field not in data or data[field] is None or str(data[field]).strip() == '':
            log_error('jobs', row_num, f'Campo requerido faltante: {field}', data)
            return False
    
    try:
        int(data['id'])
    except (ValueError, TypeError):
        log_error('jobs', row_num, 'ID debe ser número entero', data)
        return False
    
    return True


# ==================== ENDPOINTS PARA CARGA DE CSV ====================

@app.route('/api/upload', methods=['POST'])
def upload_csv():
    """
    Cargar datos CSV históricos a la BD
    Soporta: departments, jobs, hired_employees
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV'}), 400
    
    try:
        # Leer CSV
        df = pd.read_csv(file, header=None)
        
        # Detectar tabla por nombre de archivo
        table_type = None
        if 'department' in file.filename.lower():
            table_type = 'departments'
            df.columns = ['id', 'department']
        elif 'job' in file.filename.lower():
            table_type = 'jobs'
            df.columns = ['id', 'job']
        elif 'employee' in file.filename.lower():
            table_type = 'hired_employees'
            df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
        else:
            return jsonify({'error': 'Nombre de archivo no reconocido'}), 400
        
        # Limpiar espacios en blanco
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Validar y cargar
        conn = get_db_connection()
        cursor = conn.cursor()
        inserted = 0
        failed = 0
        
        for idx, row in df.iterrows():
            data = row.to_dict()
            
            # Validar según tabla
            valid = False
            if table_type == 'hired_employees':
                valid = validate_employee(data, idx + 1)
            elif table_type == 'departments':
                valid = validate_department(data, idx + 1)
            elif table_type == 'jobs':
                valid = validate_job(data, idx + 1)
            
            if valid:
                try:
                    if table_type == 'hired_employees':
                        cursor.execute(
                            'INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)',
                            (int(data['id']), data['name'], data['datetime'], int(data['department_id']), int(data['job_id']))
                        )
                    elif table_type == 'departments':
                        cursor.execute(
                            'INSERT INTO departments (id, department) VALUES (?, ?)',
                            (int(data['id']), data['department'])
                        )
                    elif table_type == 'jobs':
                        cursor.execute(
                            'INSERT INTO jobs (id, job) VALUES (?, ?)',
                            (int(data['id']), data['job'])
                        )
                    inserted += 1
                except pyodbc.IntegrityError as e:
                    log_error(table_type, idx + 1, f'Error de BD: {str(e)}', data)
                    failed += 1
            else:
                failed += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'message': f'Carga completada',
            'table': table_type,
            'inserted': inserted,
            'failed': failed,
            'total': len(df)
        }), 200
        
    except Exception as e:
        logger.error(f"Error en upload: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== ENDPOINTS PARA API REST (INSERCIONES) ====================

@app.route('/api/employees', methods=['POST'])
def insert_employees():
    """Insertar empleados (hasta 1000 registros por request)"""
    return insert_batch('hired_employees')


@app.route('/api/departments', methods=['POST'])
def insert_departments():
    """Insertar departamentos"""
    return insert_batch('departments')


@app.route('/api/jobs', methods=['POST'])
def insert_jobs():
    """Insertar trabajos"""
    return insert_batch('jobs')


def insert_batch(table: str):
    """Función genérica para insertar en lotes"""
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({'error': 'Los datos deben ser una lista de registros'}), 400
        
        if len(data) > MAX_BATCH_SIZE:
            return jsonify({'error': f'Máximo {MAX_BATCH_SIZE} registros por request'}), 400
        
        if len(data) == 0:
            return jsonify({'error': 'Lista vacía'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        inserted = 0
        failed = 0
        errors = []
        
        for idx, record in enumerate(data, 1):
            valid = False
            
            if table == 'hired_employees':
                valid = validate_employee(record, idx)
            elif table == 'departments':
                valid = validate_department(record, idx)
            elif table == 'jobs':
                valid = validate_job(record, idx)
            
            if valid:
                try:
                    if table == 'hired_employees':
                        cursor.execute(
                            'INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)',
                            (record['id'], record['name'], record['datetime'], record['department_id'], record['job_id'])
                        )
                    elif table == 'departments':
                        cursor.execute(
                            'INSERT INTO departments (id, department) VALUES (?, ?)',
                            (record['id'], record['department'])
                        )
                    elif table == 'jobs':
                        cursor.execute(
                            'INSERT INTO jobs (id, job) VALUES (?, ?)',
                            (record['id'], record['job'])
                        )
                    inserted += 1
                except pyodbc.IntegrityError as e:
                    log_error(table, idx, str(e), record)
                    failed += 1
                    errors.append(f"Registro {idx}: {str(e)}")
            else:
                failed += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'message': 'Inserción completada',
            'table': table,
            'inserted': inserted,
            'failed': failed,
            'total': len(data),
            'errors': errors if errors else []
        }), 200
        
    except Exception as e:
        logger.error(f"Error en insert_batch: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== ENDPOINTS PARA BACKUP Y RESTORE ====================

@app.route('/api/backup/<table_name>', methods=['POST'])
def backup_table(table_name: str):
    """Crear backup de una tabla en formato AVRO"""
    valid_tables = ['hired_employees', 'departments', 'jobs']
    
    if table_name not in valid_tables:
        return jsonify({'error': 'Tabla no válida'}), 400
    
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        conn.close()
        
        if df.empty:
            return jsonify({'error': 'Tabla vacía'}), 400
        
        # Crear archivo AVRO
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{BACKUP_DIR}/{table_name}_{timestamp}.parquet'
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
        
        logger.info(f"Backup creado: {filename}")
        return jsonify({
            'message': 'Backup creado exitosamente',
            'table': table_name,
            'filename': filename,
            'rows': len(df)
        }), 200
        
    except Exception as e:
        logger.error(f"Error en backup: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/restore/<table_name>', methods=['POST'])
def restore_table(table_name: str):
    """Restaurar una tabla desde backup"""
    valid_tables = ['hired_employees', 'departments', 'jobs']
    
    if table_name not in valid_tables:
        return jsonify({'error': 'Tabla no válida'}), 400
    
    data = request.get_json()
    backup_file = data.get('backup_file')
    
    if not backup_file:
        return jsonify({'error': 'backup_file requerido'}), 400
    
    try:
        # Leer archivo AVRO
        df = pd.read_parquet(backup_file)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla actual
        cursor.execute(f'DELETE FROM {table_name}')
        
        # Insertar datos del backup
        for _, row in df.iterrows():
            if table_name == 'hired_employees':
                cursor.execute(
                    'INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)',
                    (row['id'], row['name'], row['datetime'], row['department_id'], row['job_id'])
                )
            elif table_name == 'departments':
                cursor.execute(
                    'INSERT INTO departments (id, department) VALUES (?, ?)',
                    (row['id'], row['department'])
                )
            elif table_name == 'jobs':
                cursor.execute(
                    'INSERT INTO jobs (id, job) VALUES (?, ?)',
                    (row['id'], row['job'])
                )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Tabla {table_name} restaurada desde {backup_file}")
        return jsonify({
            'message': 'Tabla restaurada exitosamente',
            'table': table_name,
            'rows_restored': len(df)
        }), 200
        
    except Exception as e:
        logger.error(f"Error en restore: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== ENDPOINTS PARA ANÁLISIS (DESAFÍO #2) ====================

@app.route('/api/analytics/employees-by-quarter', methods=['GET'])
def employees_by_quarter():
    """
    Empleados contratados por trabajo y departamento, dividido por trimestres en 2021
    Ordenado alfabéticamente por departamento y trabajo
    """
    try:
        conn = get_db_connection()
        
        query = '''
        SELECT 
            d.department,
            j.job,
            CASE 
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('01', '02', '03') THEN 'Q1'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('04', '05', '06') THEN 'Q2'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('07', '08', '09') THEN 'Q3'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('10', '11', '12') THEN 'Q4'
            END as quarter,
            COUNT(*) as count
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE SUBSTRING(he.datetime, 1, 4) = '2021'
        GROUP BY 
            d.department, 
            j.job, 
            CASE 
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('01', '02', '03') THEN 'Q1'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('04', '05', '06') THEN 'Q2'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('07', '08', '09') THEN 'Q3'
                WHEN SUBSTRING(he.datetime, 6, 2) IN ('10', '11', '12') THEN 'Q4'
            END
        ORDER BY d.department ASC, j.job ASC, quarter ASC
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Pivotar para obtener formato Q1, Q2, Q3, Q4
        pivot_df = df.pivot_table(
            index=['department', 'job'],
            columns='quarter',
            values='count',
            fill_value=0,
            aggfunc='sum'
        )
        
        # Asegurar que existan todas las columnas de trimestres
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            if q not in pivot_df.columns:
                pivot_df[q] = 0
        
        pivot_df = pivot_df[['Q1', 'Q2', 'Q3', 'Q4']]
        pivot_df = pivot_df.reset_index()
        
        result = []
        for _, row in pivot_df.iterrows():
            result.append({
                'department': row['department'],
                'job': row['job'],
                'Q1': int(row['Q1']),
                'Q2': int(row['Q2']),
                'Q3': int(row['Q3']),
                'Q4': int(row['Q4'])
            })
        
        return jsonify({
            'title': 'Empleados contratados por trabajo y departamento, por trimestre (2021)',
            'data': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error en analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/analytics/departments-above-mean', methods=['GET'])
def departments_above_mean():
    """
    Departamentos que contrataron más empleados que el promedio en 2021
    Ordenado por número de empleados (descendente)
    """
    try:
        conn = get_db_connection()
        
        # Calcular cantidad de empleados por departamento
        query = '''
        SELECT 
            d.id,
            d.department,
            COUNT(*) as hired
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE SUBSTRING(he.datetime, 1, 4) = '2021'
        GROUP BY d.id, d.department
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({'error': 'No hay datos para 2021'}), 400
        
        # Calcular promedio
        mean_hired = df['hired'].mean()
        
        # Filtrar departamentos sobre el promedio
        above_mean = df[df['hired'] > mean_hired].sort_values('hired', ascending=False)
        
        result = []
        for _, row in above_mean.iterrows():
            result.append({
                'id': int(row['id']),
                'department': row['department'],
                'hired': int(row['hired'])
            })
        
        return jsonify({
            'title': 'Departamentos que contrataron más que el promedio en 2021',
            'mean_hired': float(mean_hired),
            'data': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error en analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== ENDPOINTS ÚTILES ====================

@app.route('/api/status', methods=['GET'])
def status():
    """Ver estado general de las tablas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tables_info = {}
        for table in ['hired_employees', 'departments', 'jobs']:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            tables_info[table] = count
        
        conn.close()
        
        return jsonify({
            'status': 'OK',
            'database': DB_NAME,
            'tables': tables_info
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/backups', methods=['GET'])
def list_backups():
    """Listar todos los backups disponibles"""
    try:
        if not os.path.exists(BACKUP_DIR):
            return jsonify({'backups': []}), 200
        
        files = os.listdir(BACKUP_DIR)
        backups = [f for f in files if f.endswith('.parquet')]
        
        return jsonify({
            'backup_dir': BACKUP_DIR,
            'backups': backups
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ==================== INICIALIZACIÓN ====================

if __name__ == '__main__':
    init_database()
    app.run(debug=True, host='0.0.0.0', port=5001)
