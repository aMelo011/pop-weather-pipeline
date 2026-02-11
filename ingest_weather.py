import requests
import psycopg2
from datetime import datetime
import sys

# Configuração da Base de Dados (A mesma do teste)
DB_CONFIG = {
    "dbname": "data_test",
    "user": "melo",
    "password": "root",
    "host": "localhost",
    "port": "5432"
}

# Configuração da API (Lisboa)
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=38.71&longitude=-9.13&current=temperature_2m,relative_humidity_2m,wind_speed_10m"

def extract_data():
    """Extrai dados da API OpenMeteo"""
    print("📡 A contactar a API...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Lança erro se a API falhar (404, 500)
        data = response.json()
        
        # Selecionar apenas o que nos interessa (Parsing)
        current = data['current']
        clean_data = {
            "city": "Lisboa",
            "temperature": current['temperature_2m'],
            "humidity": current['relative_humidity_2m'],
            "wind_speed": current['wind_speed_10m']
        }
        return clean_data
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        sys.exit(1) # Sai do programa com erro

def load_data(data):
    """Carrega os dados para o Postgres"""
    print("💾 A guardar na Base de Dados...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        query = """
            INSERT INTO weather_data (city, temperature, humidity, wind_speed)
            VALUES (%s, %s, %s, %s);
        """
        
        cur.execute(query, (data['city'], data['temperature'], data['humidity'], data['wind_speed']))
        conn.commit() # Importante! Sem isto os dados não ficam gravados.
        
        print(f"✅ Sucesso! Dados inseridos: {data}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao guardar: {e}")

if __name__ == "__main__":
    weather = extract_data()
    load_data(weather)
