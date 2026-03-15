import tomli
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

print("1. Conectando a Supabase...")
with open("../config/secrets.toml", "rb") as f:
    config = tomli.load(f)

db_config = config["database"]
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(db_url)

# ==========================================
# 2. DIM EMPLEADO: Agregando reglas de negocio
# ==========================================
print("\nTransformando datos para gold.dim_empleado...")
df_emp = pd.read_sql("SELECT * FROM silver.empleados", engine)

if not df_emp.empty:
    # Regla 1: Unir nombre y apellido
    df_emp['nombre_completo'] = df_emp['nombre'] + ' ' + df_emp['apellido']
    
    # Regla 2: Calcular años de antigüedad
    hoy = pd.to_datetime(datetime.now())
    df_emp['fecha_ingreso'] = pd.to_datetime(df_emp['fecha_ingreso'])
    df_emp['antiguedad_anios'] = ((hoy - df_emp['fecha_ingreso']).dt.days / 365.25).astype(int)
    
    # Seleccionar columnas finales
    cols_gold_emp = ['id_empleado', 'nombre_completo', 'departamento', 'salario', 'antiguedad_anios']
    df_gold_emp = df_emp[cols_gold_emp]
    
    df_gold_emp.to_sql('dim_empleado', engine, schema='gold', if_exists='append', index=False)
    print("¡Éxito! dim_empleado cargada en Gold.")

# ==========================================
# 3. DIM MANAGER: Pase directo
# ==========================================
print("\nTransformando datos para gold.dim_manager...")
df_man = pd.read_sql("SELECT * FROM silver.manager", engine)

if not df_man.empty:
    cols_gold_man = ['id_manager', 'nombre_completo', 'area_asignada', 'nivel_jerarquico']
    df_gold_man = df_man[cols_gold_man]
    
    df_gold_man.to_sql('dim_manager', engine, schema='gold', if_exists='append', index=False)
    print("¡Éxito! dim_manager cargada en Gold.")

# ==========================================
# 4. DIM PRODUCTO: Estado de inventario
# ==========================================
print("\nTransformando datos para gold.dim_producto...")
df_prod = pd.read_sql("SELECT * FROM silver.producto", engine)

if not df_prod.empty:
    # Regla de negocio: Categorizar el stock
    def categorizar_stock(stock):
        if stock < 50: return 'Crítico'
        elif stock < 150: return 'Bajo'
        else: return 'Óptimo'
        
    df_prod['estado_stock'] = df_prod['stock_disponible'].apply(categorizar_stock)
    
    cols_gold_prod = ['id_producto', 'nombre_producto', 'categoria', 'precio', 'estado_stock']
    df_gold_prod = df_prod[cols_gold_prod]
    
    df_gold_prod.to_sql('dim_producto', engine, schema='gold', if_exists='append', index=False)
    print("¡Éxito! dim_producto cargada en Gold.")