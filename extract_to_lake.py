import requests
import polars as pl
from datetime import datetime
import os

# Configurações
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=38.71&longitude=-9.13&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
LAKE_PATH = "data_lake"

def extract_data():
    print("📡 A contactar API...")
    response = requests.get(API_URL)
    data = response.json()
    current = data['current']
    
    # Criar um Dicionário com estrutura
    row = {
        "city": "Lisboa",
        "temperature": current['temperature_2m'],
        "humidity": current['relative_humidity_2m'],
        "wind_speed": current['wind_speed_10m'],
        "extracted_at": datetime.now()
    }
    return row

def save_to_parquet(row):
    # 1. Converter para DataFrame do Polars (Tabela em memória)
    df = pl.DataFrame([row])
    
    # 2. Criar nome do ficheiro baseado na data (Particionamento)
    # Ex: data_lake/weather_2026-02-11.parquet
    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(LAKE_PATH, exist_ok=True)
    file_path = f"{LAKE_PATH}/weather_{today}.parquet"
    
    print(f"💾 A gravar em: {file_path}")
    
    # Lógica de "Append" (Se o ficheiro já existe, adicionamos; se não, criamos)
    if os.path.exists(file_path):
        # Ler o existente e juntar o novo (Concatenação)
        df_existing = pl.read_parquet(file_path)
        df_final = pl.concat([df_existing, df])
        df_final.write_parquet(file_path)
    else:
        # Criar novo
        df.write_parquet(file_path)

if __name__ == "__main__":
    data = extract_data()
    save_to_parquet(data)
    print("✅ Sucesso! Dados no Data Lake.")
