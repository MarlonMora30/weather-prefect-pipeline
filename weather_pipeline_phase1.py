"""
Pipeline didactico:
Open-Meteo (API clima) -> pandas (staging) -> transform -> MongoDB

Open-Meteo:
- Forecast docs: https://open-meteo.com/en/docs
- Geocoding docs: https://open-meteo.com/en/docs/geocoding-api
"""

import os
from datetime import datetime, timezone

import requests
import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv


#---------------------
# CONFIG
#---------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "weather_demo")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "hourly_weather")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


#---------------------
# 1) EXTRACT - Geocoding
#---------------------
def get_lat_lon(city: str, country_code: str = "CR") -> tuple[float, float, str]:

    params = {
        "name": city,
        "count": 1,
        "language": "es",
        "format": "json",
        "country_code": country_code,
    }

    r = requests.get(GEOCODING_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "results" not in data or len(data["results"]) == 0:
        raise ValueError(f"No se encontraron resultados para city={city}, country_code={country_code}")

    top = data["results"][0]
    return float(top["latitude"]), float(top["longitude"]), top["name"]


#---------------------
# 2) EXTRACT - Weather Forecast
#---------------------
def fetch_hourly_weather(lat: float, lon: float, timezone_name: str = "America/Costa_Rica") -> dict:

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": timezone_name,
    }

    r = requests.get(FORECAST_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


#---------------------
# 3) STAGE - dict/json -> DataFrame temporal
#---------------------
def to_staging_df(weather_json: dict, location_name: str) -> pd.DataFrame:

    hourly = weather_json.get("hourly", {})

    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "precipitation": hourly.get("precipitation", []),
        "windspeed_10m": hourly.get("windspeed_10m", []),
    })

    df["location_name"] = location_name
    df["latitude"] = weather_json.get("latitude")
    df["longitude"] = weather_json.get("longitude")

    df["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

    return df


#---------------------
# 4) Transform - limpieza + tipos + features
#---------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:

    out = df.copy()

    out["time"] = pd.to_datetime(out["time"], errors="coerce")

    for col in ["temperature_2m", "precipitation", "windspeed_10m"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["time", "temperature_2m"])

    out["is_rain"] = out["precipitation"].fillna(0) > 0
    out["date"] = out["time"].dt.date.astype(str)

    return out


#---------------------
# 5) SNAPSHOT STAGING - DataFrame -> JSON temporal
#---------------------
def save_staging_snapshot(df: pd.DataFrame) -> str:
    """
    Guarda el DataFrame staging de cada corrida en:
    temporal/fecha_hora/staging.json
    """

    run_folder = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    out_dir = os.path.join(BASE_DIR, "temporal", run_folder)
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "staging.json")

    df.to_json(out_file, orient="records", indent=2, force_ascii=False)

    return out_file


#---------------------
# 6) LOAD - DataFrame -> MongoDB
#---------------------
def load_to_mongo(df: pd.DataFrame) -> int:

    if df.empty:
        return 0

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    col.create_index(
        [("location_name", ASCENDING), ("time", ASCENDING)],
        unique=True
    )

    records = df.to_dict(orient="records")

    inserted = 0

    for doc in records:
        col.update_one(
            {"location_name": doc["location_name"], "time": doc["time"]},
            {"$set": doc},
            upsert=True
        )
        inserted += 1

    client.close()
    return inserted


#---------------------
# RUN PIPELINE
#---------------------
def run(city: str = "San José", country_code: str = "CR"):

    # 1) geocoding
    lat, lon, resolved_name = get_lat_lon(city=city, country_code=country_code)

    # 2) extract weather
    weather_json = fetch_hourly_weather(lat, lon)

    # 3) staging df
    staging_df = to_staging_df(weather_json, location_name=resolved_name)
    print("STAGING DF {head}:")
    print(staging_df.head())

    # 4) transform
    final_df = transform(staging_df)
    print("\nTRANSFORMED DF (head):")
    print(final_df.head())

    # 5) snapshot staging
    snapshot_path = save_staging_snapshot(staging_df)
    print(f"\nSnapshot staging guardado en: {snapshot_path}")

    # 6) load
    n = load_to_mongo(final_df)
    print(f"\nListo Registros upserted (insert/update): {n}")
    print(f"Destino: {DB_NAME}.{COLLECTION_NAME}")


if __name__ == "__main__":
    run()