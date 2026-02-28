# 🌦 Weather Data Pipeline with Prefect

Este proyecto implementa un pipeline ETL automatizado para la extracción, transformación y carga de datos climáticos utilizando Prefect 3 como orquestador.

## 🚀 Descripción

El pipeline:

1. Extrae datos desde una API de clima.
2. Genera un archivo staging en formato JSON.
3. Transforma los datos en un DataFrame limpio.
4. Ejecuta el flujo de manera programada usando Prefect.
5. Permite ejecución manual desde la UI o CLI.

El flujo está programado para ejecutarse diariamente a las 6:00 AM (zona horaria America/Costa_Rica).

---

## 🏗 Arquitectura

API → Extract → Staging JSON → Transform → Prefect Flow → Deployment → Scheduler

---

## 🛠 Tecnologías utilizadas

- Python 3.13
- Prefect 3
- Pandas
- Git & GitHub

---

## ⚙️ Cómo ejecutar el proyecto localmente

### 1️⃣ Crear entorno virtual

```bash
python -m venv venv
venv\Scripts\activate

### 2️⃣ Instalar dependencias

pip install -r requirements.txt

### 3️⃣ Iniciar Prefect Server

prefect server start

### 4️⃣ Servir el flow

python weather_pipeline_prefect.py

### ▶ Ejecutar manualmente

prefect deployment run "weather-prefect-pipeline/weather-daily-deployment"

## ☁️ Despliegue en Prefect Cloud

El proyecto puede desplegarse en Prefect Cloud utilizando:

prefect cloud login
prefect deploy

