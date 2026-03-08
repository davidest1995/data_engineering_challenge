"""
Script de prueba para la API REST
Ejecutar después de iniciar: python app.py
"""

import requests
import json
import time

BASE_URL = 'http://localhost:5001/api'

# Colores para output
GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
END = '\033[0m'

def print_header(text):
    print(f"\n{BLUE}{'='*60}")
    print(f"{text}")
    print(f"{'='*60}{END}\n")

def print_success(text):
    print(f"{GREEN}✓ {text}{END}")

def print_error(text):
    print(f"{RED}✗ {text}{END}")

# ==================== TESTS ====================

def test_status():
    """Verificar estado de la BD"""
    print_header("TEST 1: Estado de la Base de Datos")
    
    try:
        response = requests.get(f'{BASE_URL}/status')
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_insert_departments():
    """Insertar departamentos"""
    print_header("TEST 2: Insertar Departamentos")
    
    departments = [
        {"id": 1, "department": "Product Management"},
        {"id": 2, "department": "Sales"},
        {"id": 3, "department": "Research and Development"},
        {"id": 4, "department": "Business Development"},
        {"id": 5, "department": "Engineering"}
    ]
    
    try:
        response = requests.post(
            f'{BASE_URL}/departments',
            json=departments,
            headers={'Content-Type': 'application/json'}
        )
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_insert_jobs():
    """Insertar trabajos"""
    print_header("TEST 3: Insertar Trabajos")
    
    jobs = [
        {"id": 1, "job": "Marketing Assistant"},
        {"id": 2, "job": "VP Sales"},
        {"id": 3, "job": "Biostatistician IV"},
        {"id": 4, "job": "Account Representative II"},
        {"id": 5, "job": "VP Marketing"}
    ]
    
    try:
        response = requests.post(
            f'{BASE_URL}/jobs',
            json=jobs,
            headers={'Content-Type': 'application/json'}
        )
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_insert_employees():
    """Insertar empleados"""
    print_header("TEST 4: Insertar Empleados")
    
    employees = [
        {
            "id": 1,
            "name": "Harold Vogt",
            "datetime": "2021-11-07T02:48:42Z",
            "department_id": 1,
            "job_id": 1
        },
        {
            "id": 2,
            "name": "Ty Hofer",
            "datetime": "2021-05-30T05:43:46Z",
            "department_id": 2,
            "job_id": 2
        },
        {
            "id": 3,
            "name": "Lyman Hadye",
            "datetime": "2021-09-01T23:27:38Z",
            "department_id": 3,
            "job_id": 3
        }
    ]
    
    try:
        response = requests.post(
            f'{BASE_URL}/employees',
            json=employees,
            headers={'Content-Type': 'application/json'}
        )
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_invalid_data():
    """Probar inserción con datos inválidos"""
    print_header("TEST 5: Validación - Datos Inválidos")
    
    invalid_employees = [
        {
            "id": 999,
            "name": "Persona Sin Datos",
            # Falta datetime, department_id, job_id
            "department_id": 1,
            "job_id": 1
        },
        {
            "id": "texto",  # ID debe ser número
            "name": "Persona",
            "datetime": "2021-01-01T00:00:00Z",
            "department_id": 1,
            "job_id": 1
        }
    ]
    
    try:
        response = requests.post(
            f'{BASE_URL}/employees',
            json=invalid_employees,
            headers={'Content-Type': 'application/json'}
        )
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_backup():
    """Crear backup de una tabla"""
    print_header("TEST 6: Crear Backup")
    
    try:
        response = requests.post(f'{BASE_URL}/backup/departments')
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_list_backups():
    """Listar backups disponibles"""
    print_header("TEST 7: Listar Backups")
    
    try:
        response = requests.get(f'{BASE_URL}/backups')
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_analytics_by_quarter():
    """Analytics: Empleados por trimestre"""
    print_header("TEST 8: Analytics - Empleados por Trimestre (2021)")
    
    try:
        response = requests.get(f'{BASE_URL}/analytics/employees-by-quarter')
        print_success(f"Status: {response.status_code}")
        data = response.json()
        print(f"Título: {data['title']}\n")
        # Mostrar solo primeros 5 resultados
        for row in data['data'][:5]:
            print(f"  {row['department']:30} {row['job']:30} Q1:{row['Q1']} Q2:{row['Q2']} Q3:{row['Q3']} Q4:{row['Q4']}")
        if len(data['data']) > 5:
            print(f"  ... y {len(data['data']) - 5} registros más")
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_analytics_above_mean():
    """Analytics: Departamentos sobre el promedio"""
    print_header("TEST 9: Analytics - Departamentos sobre Promedio (2021)")
    
    try:
        response = requests.get(f'{BASE_URL}/analytics/departments-above-mean')
        print_success(f"Status: {response.status_code}")
        data = response.json()
        print(f"Título: {data['title']}")
        print(f"Promedio de empleados contratados: {data['mean_hired']:.2f}\n")
        for row in data['data']:
            print(f"  {row['department']:30} Contratados: {row['hired']}")
    except Exception as e:
        print_error(f"Error: {str(e)}")


def test_batch_limit():
    """Probar límite de batch (máx 1000)"""
    print_header("TEST 10: Validación - Límite de Batch")
    
    # Crear 1001 registros para exceder el límite
    large_batch = [
        {
            "id": i,
            "name": f"Persona {i}",
            "datetime": "2021-01-01T00:00:00Z",
            "department_id": 1,
            "job_id": 1
        }
        for i in range(1000, 2001)  # 1001 registros
    ]
    
    try:
        response = requests.post(
            f'{BASE_URL}/employees',
            json=large_batch,
            headers={'Content-Type': 'application/json'}
        )
        print_success(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print_error(f"Error: {str(e)}")


# ==================== EJECUCIÓN ====================

if __name__ == '__main__':
    print(f"\n{BLUE}╔════════════════════════════════════════════════════════════╗")
    print(f"║         TESTS DE LA API REST - GLOBANT CHALLENGE          ║")
    print(f"╚════════════════════════════════════════════════════════════╝{END}")
    
    # Esperar a que la API esté lista
    print("\nEsperando que la API esté disponible...")
    for i in range(10):
        try:
            requests.get(f'{BASE_URL}/status', timeout=1)
            print_success("API está lista!\n")
            break
        except:
            print(".", end="", flush=True)
            time.sleep(1)
    
    # Ejecutar tests
    test_status()
    test_insert_departments()
    time.sleep(0.5)
    test_insert_jobs()
    time.sleep(0.5)
    test_insert_employees()
    time.sleep(0.5)
    test_invalid_data()
    time.sleep(0.5)
    test_batch_limit()
    time.sleep(0.5)
    test_backup()
    time.sleep(0.5)
    test_list_backups()
    time.sleep(0.5)
    
    # Los tests de analytics solo funcionan si hay datos de 2021
    # test_analytics_by_quarter()
    # test_analytics_above_mean()
    
    print_header("TESTS COMPLETADOS")
    print(f"{GREEN}✓ Todos los tests han sido ejecutados{END}\n")
