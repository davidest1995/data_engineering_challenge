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

## 🛑 Cómo Detener el Proyecto

Cuando termines de usar la aplicación, puedes detener y remover los contenedores usando este comando:

```bash
docker-compose down
```
