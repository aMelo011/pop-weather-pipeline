import sys
import subprocess
import time

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

print("🚀 A iniciar verificação do Stack no Pop!_OS...")

# 1. Verificar Python
print(f"🐍 Python Version: {sys.version.split()[0]}")

# 2. Garantir driver (auto-install para o teste)
try:
    import psycopg2
except ImportError:
    print("📦 A instalar driver Postgres (psycopg2-binary)...")
    install("psycopg2-binary")
    import psycopg2

# 3. Conectar ao Docker
print("🔌 A tentar conectar ao Postgres no Docker...")
config = {
    "dbname": "data_test",
    "user": "icaro",
    "password": "popos_rules",
    "host": "localhost",
    "port": "5432"
}

connected = False
for i in range(5): # Tenta 5 vezes (caso o Docker esteja a acordar)
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"✅ SUCESSO! Conectado a: {version}")
        conn.close()
        connected = True
        break
    except Exception as e:
        print(f"⏳ Tentativa {i+1}/5 falhou... a esperar que o Docker acorde.")
        time.sleep(2)

if not connected:
    print("❌ FALHA CRÍTICA: Não foi possível conectar ao Docker.")
else:
    print("🎉 O SISTEMA ESTÁ 100% OPERACIONAL. Pronto para Engenharia de Dados.")
