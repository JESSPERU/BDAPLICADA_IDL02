import psycopg2
import tomli
import pandas as pd
from sqlalchemy import create_engine

# 1. Leer las credenciales ocultas del archivo secrets.toml
with open("../config/secrets.toml", "rb") as f:
    config = tomli.load(f)

db_config = config["database"]

# 2. Crear la cadena de conexión para SQLAlchemy
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

try:
    # 3. Conectarse a la base de datos
    engine = create_engine(db_url)
    conexion = engine.connect()
    print("¡Conexión a Supabase exitosa! El motor está listo.")
    
    # Prueba rápida: Leer qué hay en la capa Bronze
    df_prueba = pd.read_sql("SELECT * FROM bronze.empleados", conexion)
    print("\nDatos actuales en bronze.empleados:")
    print(df_prueba.head())
    
    conexion.close()
except Exception as e:
    print(f"Error al conectar: {e}")