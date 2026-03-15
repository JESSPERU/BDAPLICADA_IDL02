import tomli
import pandas as pd
import random
from sqlalchemy import create_engine

print("1. Conectando a Supabase...")
with open("../config/secrets.toml", "rb") as f:
    config = tomli.load(f)

db_config = config["database"]
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(db_url)

df_emp = pd.read_sql("SELECT id_empleado FROM silver.empleados", engine)
df_prod = pd.read_sql("SELECT id_producto, precio FROM silver.producto", engine)

if not df_emp.empty and not df_prod.empty:
    print("2. Generando tabla de hechos (FAC)...")
    
    lista_empleados = df_emp['id_empleado'].tolist()
    
    transacciones = []
    for i in range(1, 201):
        producto_random = df_prod.sample(1).iloc[0]
        cantidad = random.randint(1, 5)
        
        dia = random.randint(1, 15)
        # Mantenemos el año 26
        fecha_venta = f"26-03-{dia:02d}" 
        
        transacciones.append({
            'id_empleado': random.choice(lista_empleados),
            'id_producto': int(producto_random['id_producto']), # Convertido a entero para evitar el .0
            'fecha_venta': fecha_venta,
            'cantidad': cantidad,
            'monto_total': round(producto_random['precio'] * cantidad, 2)
        })
    
    df_fac_ventas = pd.DataFrame(transacciones)
    
    # TRADUCCIÓN DE FECHA: Le decimos a Pandas que el '26' inicial es el año (%y)
    df_fac_ventas['fecha_venta'] = pd.to_datetime(df_fac_ventas['fecha_venta'], format='%y-%m-%d')
    
    print("3. Cargando gold.fac_ventas...")
    try:
        df_fac_ventas.to_sql('fac_ventas', engine, schema='gold', if_exists='append', index=False)
        print("¡Éxito! Tu tabla de hechos ha sido cargada. Arquitectura completada.")
    except Exception as e:
        print(f"Error al cargar FAC: {e}")