"""
Script para cargar los archivos CSV iniciales a la BD
Usa el endpoint /api/upload para importar los datos históricos
"""

import requests
import json
import time
import os

BASE_URL = 'http://localhost:5001/api'
#local
#UPLOAD_DIR = '../data/'
#docker
UPLOAD_DIR = 'data/'


# Colores para output
GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
YELLOW = '\033[93m'
END = '\033[0m'

def print_header(text):
    print(f"\n{BLUE}{'='*60}")
    print(f"{text}")
    print(f"{'='*60}{END}\n")

def print_success(text):
    print(f"{GREEN}✓ {text}{END}")

def print_error(text):
    print(f"{RED}✗ {text}{END}")

def print_info(text):
    print(f"{YELLOW}ℹ {text}{END}")

def upload_csv(filepath, filename):
    """Subir un archivo CSV a la API"""
    try:
        with open(filepath, 'rb') as f:
            files = {'file': (filename, f, 'text/csv')}
            response = requests.post(f'{BASE_URL}/upload', files=files)
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"{filename}")
            print(f"  └─ Insertados: {data['inserted']}, Fallidos: {data['failed']}, Total: {data['total']}")
            return True
        else:
            print_error(f"{filename}: {response.json()}")
            return False
    except FileNotFoundError:
        print_error(f"Archivo no encontrado: {filepath}")
        return False
    except Exception as e:
        print_error(f"Error al cargar {filename}: {str(e)}")
        return False

def check_api():
    """Verificar que la API esté disponible"""
    try:
        response = requests.get(f'{BASE_URL}/status', timeout=2)
        return response.status_code == 200
    except:
        return False

# ==================== EJECUCIÓN ====================

if __name__ == '__main__':
    print_header("CARGADOR DE DATOS CSV - GLOBANT CHALLENGE")
    
    # Verificar que la API esté corriendo
    print("Verificando disponibilidad de la API...")
    if not check_api():
        print_error("La API no está disponible en http://localhost:5001")
        print(f"{YELLOW}Ejecuta primero: python app.py{END}")
        exit(1)
    
    print_success("API disponible!\n")
    
    # Archivos a cargar (en orden)
    files_to_load = [
        ('departments.csv', 'departments.csv'),
        ('jobs.csv', 'jobs.csv'),
        ('hired_employees.csv', 'hired_employees.csv')
    ]
    
    print_header("Iniciando carga de archivos CSV")
    
    successful = 0
    failed = 0
    
    for filename, display_name in files_to_load:
        filepath = os.path.join(UPLOAD_DIR, filename)
        
        if os.path.exists(filepath):
            print(f"Cargando {display_name}...")
            if upload_csv(filepath, display_name):
                successful += 1
            else:
                failed += 1
            time.sleep(1)
        else:
            print_error(f"No se encontró: {filepath}")
            failed += 1
    
    # Resumen
    print_header("Resumen de carga")
    print_success(f"Archivos cargados exitosamente: {successful}")
    if failed > 0:
        print_error(f"Archivos con error: {failed}")
    
    # Mostrar estado final
    print_info("Verificando estado de la BD...")
    try:
        response = requests.get(f'{BASE_URL}/status')
        data = response.json()
        print(f"\nEstado actual de la base de datos:")
        print(f"  • Departamentos: {data['tables']['departments']} registros")
        print(f"  • Trabajos: {data['tables']['jobs']} registros")
        print(f"  • Empleados: {data['tables']['hired_employees']} registros")
    except Exception as e:
        print_error(f"Error al consultar estado: {str(e)}")
    
    print(f"\n{GREEN}Carga completada!{END}\n")
    print("Ahora puedes:")
    print("  1. Probar la API: python test_api.py")
    print("  2. Ver logs en: app.log y data_errors.log")
    print("  3. Ver backups en: ./backups/\n")
