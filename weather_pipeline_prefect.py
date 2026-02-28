"""
Pipeline con Prefect:
Open-Meteo -> pandas -> transform -> MongoDB
"""

from datetime import datetime
from prefect import flow, task

# 👇 IMPORTAMOS DESDE TU ARCHIVO REAL
from weather_pipeline_phase1 import (
    get_lat_lon,
    fetch_hourly_weather,
    to_staging_df,
    transform,
    save_staging_snapshot,
    load_to_mongo,
)


# -----------------------------
# TASKS
# -----------------------------

@task
def task_get_lat_lon(city: str, country_code: str):
    return get_lat_lon(city, country_code)


@task
def task_fetch_weather(lat: float, lon: float):
    return fetch_hourly_weather(lat, lon)


@task
def task_staging(weather_json, location_name: str):
    return to_staging_df(weather_json, location_name)


@task
def task_transform(df):
    return transform(df)


@task
def task_snapshot(df):
    return save_staging_snapshot(df)


@task
def task_load(df):
    return load_to_mongo(df)


# -----------------------------
# FLOW
# -----------------------------

@flow(name="weather-prefect-pipeline")
def weather_pipeline_flow(
    city: str = "San José",
    country_code: str = "CR",
):
    print(f"Pipeline iniciado: {datetime.now()}")

    # 1️⃣ Geocoding
    lat, lon, resolved_name = task_get_lat_lon(city, country_code)

    # 2️⃣ Weather
    weather_json = task_fetch_weather(lat, lon)

    # 3️⃣ Staging
    staging_df = task_staging(weather_json, resolved_name)

    # 4️⃣ Transform
    final_df = task_transform(staging_df)

    # 5️⃣ Snapshot
    snapshot_path = task_snapshot(staging_df)

    # 6️⃣ Load
    inserted = task_load(final_df)

    print(f"Snapshot guardado en: {snapshot_path}")
    print(f"Registros upserted: {inserted}")
    print("Pipeline finalizado correctamente")


# -----------------------------
# SCHEDULE
# -----------------------------

if __name__ == "__main__":
    weather_pipeline_flow.serve(
        name="weather-daily-deployment",
        schedule={
            "cron": "0 6 * * *",
            "timezone": "America/Costa_Rica",
        },
    )