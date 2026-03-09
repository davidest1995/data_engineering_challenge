# Data Engineering Challenge

Este proyecto despliega una solución completa utilizando Docker Compose, incluyendo una base de datos SQL Server, una API en Flask y un Dashboard interactivo en Streamlit.

## 🛠️ Requisitos Previos

Asegúrate de tener instalados los siguientes programas en tu entorno local:
- **Docker**
- **Docker Compose**

## ⚙️ Configuración Inicial

Antes de ejecutar el proyecto, revisa el archivo `.env` en el directorio raíz. Este archivo contiene las credenciales necesarias para iniciar la base de datos SQL Server.

Asegúrate de que haya un archivo `.env` con un contenido similar a este (ajusta la contraseña si es necesario):
```env
DB_SERVER=sqlserver
DB_NAME=challenge
DB_USER=sa
DB_PASSWORD=Your_Password123!
```

## 🚀 Cómo Ejecutar el Proyecto

Abre una terminal, navega hasta la carpeta raíz del proyecto (`data_engineering_challenge`) y ejecuta los siguientes pasos:

### 1. Construir e Iniciar los Servicios

Puedes construir las imágenes asegurándote de no usar el caché y luego levantar los contenedores en segundo plano (modo detached):

```bash
docker-compose build --no-cache
docker-compose up -d --build
```
> **Nota:** La primera vez que levantes los servicios, puede tardar un poco mientras se descarga la imagen de SQL Server y se instalan las dependencias de Python.

### 2. Cargar los Datos Iniciales

Una vez que los contenedores estén activos (puedes comprobarlo con `docker-compose ps`), debes ejecutar el script de carga de datos. Este comando leerá los archivos CSV ubicados en la carpeta `data/` y poblará la base de datos a través de la API:

```bash
docker-compose exec api python load_csv.py
```

### 3. Ejecutar Pruebas a la API (Opcional)

Si deseas verificar el correcto funcionamiento de los endpoints de la API, puedes ejecutar los tests unitarios con el siguiente comando:

```bash
docker-compose exec api python test_api.py
```

## 📊 Acceso a la Aplicación

Con todos los servicios corriendo, puedes acceder a la interfaz gráfica y a la API en tu navegador:

- **Dashboard (Streamlit):** [http://localhost:8501](http://localhost:8501)
- **API (Flask):** [http://localhost:5001](http://localhost:5001)

## 📡 Uso de la API (Endpoints)

La API expone varios endpoints para cargar, consultar y hacer backup de los datos:

### 1. Estado de la API y Base de Datos
- **Endpoint:** `GET /api/status`
- **Uso:** `curl http://localhost:5001/api/status`

### 2. Cargas Masivas de Datos (Batch Inserts)
Permiten insertar hasta 1000 registros por petición.
- **Departamentos:** `POST /api/departments`
- **Trabajos:** `POST /api/jobs`
- **Empleados:** `POST /api/employees`
- **Uso (Ejemplo):**
  ```bash
  curl -X POST http://localhost:5001/api/departments -H "Content-Type: application/json" -d '[{"id": 1, "department": "IT"}]'
  ```

### 3. Cargar archivo CSV directamente
También puedes subir el archivo CSV por medio de una petición a la API y el sistema identificará de qué tabla se trata por el nombre del archivo.
- **Endpoint:** `POST /api/upload`
- **Uso:** 
  ```bash
  curl -X POST -F "file=@data/jobs.csv" http://localhost:5001/api/upload
  ```

### 4. Backups y Restauración (AVRO/Parquet)
Puedes realizar respaldos de las tablas en formato Parquet que se guardarán en la carpeta `backups/` del proyecto.
- **Crear Backup:** `POST /api/backup/<table_name>` (tablas válidas: `hired_employees`, `departments`, `jobs`)
  ```bash
  docker-compose exec api curl -X POST http://localhost:5001/api/backup/departments
  ```
- **Listar Backups:** `GET /api/backups`
  ```bash
  curl http://localhost:5001/api/backups
  ```
- **Restaurar Tabla:** `POST /api/restore/<table_name>`
  ```bash
  curl -X POST http://localhost:5001/api/restore/departments -H "Content-Type: application/json" -d '{"backup_file": "/app/backups/departments_TIMESTAMP.parquet"}'
  ```

### 5. Análisis de Datos (Analytics)
- **Empleados contratados por trimestre (2021):** `GET /api/analytics/employees-by-quarter`
- **Departamentos sobre la media de contrataciones (2021):** `GET /api/analytics/departments-above-mean`

## 🛑 Cómo Detener el Proyecto

Cuando termines de usar la aplicación, puedes detener y remover los contenedores usando este comando:

```bash
docker-compose down
```
