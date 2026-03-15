import tomli
import pandas as pd
from sqlalchemy import create_engine

# 1. Configuración y Conexión
print("1. Conectando a la base de datos...")
with open("../config/secrets.toml", "rb") as f:
    config = tomli.load(f)

db_config = config["database"]
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(db_url)

# 2. Extraer datos de la capa Bronze
print("2. Extrayendo datos de bronze.empleados...")
df_empleados = pd.read_sql("SELECT * FROM bronze.empleados", engine)

# 3. Transformación (Limpieza y Normalización para Silver)
print("3. Limpiando y transformando los datos...")

# a) Limpiar el ID: Quitar la 'E' y convertir a número entero
df_empleados['id_empleado'] = df_empleados['id_empleado'].str.replace('E', '').astype(int)

# b) Convertir salario a valor numérico (float)
df_empleados['salario'] = pd.to_numeric(df_empleados['salario'])

# c) Convertir fecha_ingreso a tipo datetime real
df_empleados['fecha_ingreso'] = pd.to_datetime(df_empleados['fecha_ingreso'])

# d) Eliminar duplicados (regla de negocio básica para Silver)
df_empleados = df_empleados.drop_duplicates(subset=['id_empleado'])

# e) Seleccionar solo las columnas que van a Silver 
# (Excluimos fuente_archivo y la fecha_carga antigua de Bronze)
columnas_silver = ['id_empleado', 'nombre', 'apellido', 'departamento', 'salario', 'fecha_ingreso']
df_silver_empleados = df_empleados[columnas_silver]

# Mostrar un adelanto de cómo quedaron los datos limpios
print("\nVista previa de los datos listos para Silver:")
print(df_silver_empleados.head())

# 4. Cargar los datos limpios a la capa Silver
print("\n4. Cargando datos en silver.empleados...")
try:
    # Usamos if_exists='append' para agregar los datos a la tabla que ya creaste
    df_silver_empleados.to_sql('empleados', engine, schema='silver', if_exists='append', index=False)
    print("¡Éxito! Los datos han sido cargados a la capa Silver.")
except Exception as e:
    print(f"Error al cargar en Silver: {e}")


# ==========================================
# 5. PROCESANDO TABLA: MANAGER
# ==========================================
print("\n5. Extrayendo y limpiando datos de bronze.manager...")
df_manager = pd.read_sql("SELECT * FROM bronze.manager", engine)

if not df_manager.empty:
    # a) Limpiar ID (Quitar la 'M' y convertir a entero)
    df_manager['id_manager'] = df_manager['id_manager'].str.replace('M', '').astype(int)

    # b) Renombrar columna para corregir ortografía en Silver
    df_manager = df_manager.rename(columns={'nivel_gerarquico': 'nivel_jerarquico'})

    # c) Eliminar duplicados y seleccionar columnas (basadas en tu CSV)
    df_manager = df_manager.drop_duplicates(subset=['id_manager'])
    cols_silver_manager = ['id_manager', 'nombre_completo', 'area_asignada', 'nivel_jerarquico']
    df_silver_manager = df_manager[cols_silver_manager]

    print("Cargando datos en silver.manager...")
    try:
        df_silver_manager.to_sql('manager', engine, schema='silver', if_exists='append', index=False)
        print("¡Éxito! Datos de Manager cargados a Silver.")
    except Exception as e:
        print(f"Error al cargar Manager: {e}")
else:
    print("La tabla bronze.manager está vacía.")

# ==========================================
# 6. PROCESANDO TABLA: PRODUCTO
# ==========================================
print("\n6. Extrayendo y limpiando datos de bronze.producto...")
df_producto = pd.read_sql("SELECT * FROM bronze.producto", engine)

if not df_producto.empty:
    # a) Limpiar ID (Quitar la 'P' y convertir a entero)
    df_producto['id_producto'] = df_producto['id_producto'].str.replace('P', '').astype(int)

    # b) Convertir texto a numérico/entero
    df_producto['precio'] = pd.to_numeric(df_producto['precio'])
    df_producto['stock_disponible'] = pd.to_numeric(df_producto['stock_disponible']).astype(int)

    # c) Eliminar duplicados y seleccionar columnas (basadas en tu CSV)
    df_producto = df_producto.drop_duplicates(subset=['id_producto'])
    cols_silver_producto = ['id_producto', 'nombre_producto', 'categoria', 'precio', 'stock_disponible']
    df_silver_producto = df_producto[cols_silver_producto]

    print("Cargando datos en silver.producto...")
    try:
        df_silver_producto.to_sql('producto', engine, schema='silver', if_exists='append', index=False)
        print("¡Éxito! Datos de Producto cargados a Silver.")
    except Exception as e:
        print(f"Error al cargar Producto: {e}")
else:
    print("La tabla bronze.producto está vacía.")