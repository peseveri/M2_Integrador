from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count
import os
import sys

# Inicializa la sesión de Spark
spark = SparkSession.builder \
    .appName("etl-transform-airbnb") \
    .getOrCreate()

# --- Funciones de Transformación ---

def raw_to_silver(input_path, output_path):
    """
    Carga datos brutos, realiza limpieza y guarda en la capa Silver.
    """
    print("Iniciando transformación de Raw a Silver...")
    # El archivo de entrada es el CSV procesado
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Limpieza de datos y conversiones de tipo
    df_silver = df_raw.withColumn("price", col("price").cast("int")) \
                      .withColumn("minimum_nights", col("minimum_nights").cast("int"))
    
    df_silver = df_silver.na.fill(0, subset=['reviews_per_month'])
    df_silver = df_silver.na.drop(subset=['price', 'host_id'])
    
    # Escribe el DataFrame en la ruta de salida (capa Silver)
    df_silver.write.mode("overwrite").parquet(output_path)
    print("Transformación a Capa Silver completada.")
    
def silver_to_gold(input_path, output_path_precios, output_path_anfitriones):
    """
    Carga datos de la capa Silver, realiza agregaciones y guarda en la capa Gold.
    """
    print("Iniciando transformación de Silver a Gold...")
    # Carga los datos de la capa Silver
    df_silver = spark.read.parquet(input_path)
    
    # Pregunta 1: Precio promedio por tipo de habitación y barrio
    df_gold_precios = df_silver.groupBy("neighbourhood_group", "room_type") \
                               .agg(avg("price").alias("precio_promedio"))
    
    # Pregunta 2: Anfitriones clave
    df_gold_anfitriones = df_silver.groupBy("host_id", "host_name") \
                                   .agg(count("id").alias("total_listados"),
                                        sum("number_of_reviews").alias("total_reviews")) \
                                   .orderBy(col("total_listados").desc())
    
    # Escribe los DataFrames agregados en la capa Gold
    df_gold_precios.write.mode("overwrite").parquet(output_path_precios)
    df_gold_anfitriones.write.mode("overwrite").parquet(output_path_anfitriones)
    print("Transformación a Capa Gold completada.")

# --- Punto de Entrada del Script ---
if __name__ == "__main__":
    # Obtiene las rutas de los volúmenes de las variables de entorno
    # que Airflow le pasa al contenedor.
    RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/app/raw_data")
    SILVER_DATA_PATH = os.getenv("SILVER_DATA_PATH", "/app/silver_data")
    GOLD_DATA_PATH = os.getenv("GOLD_DATA_PATH", "/app/gold_data")
    
    # Lógica para encontrar el archivo CSV más reciente
    try:
        # Verifica si el directorio de datos brutos existe
        if not os.path.exists(RAW_DATA_PATH):
            raise FileNotFoundError(f"Error: El directorio de datos brutos no existe en la ruta: {RAW_DATA_PATH}")

        # Busca el archivo CSV con el prefijo correcto
        csv_files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith("ab_nyc_raw_processed_") and f.endswith(".csv")]
        
        if not csv_files:
            raise FileNotFoundError("No se encontró ningún archivo CSV procesado.")
        csv_files.sort(reverse=True)
        csv_file_path = os.path.join(RAW_DATA_PATH, csv_files[0])
    except FileNotFoundError as e:
        print(f"Error: {e}")
        spark.stop()
        sys.exit(1)

    print(f"Usando el archivo procesado más reciente: {csv_file_path}")

    # Define las rutas de salida con los directorios correctos
    silver_path = os.path.join(SILVER_DATA_PATH, "cleaned_airbnb_listings")
    gold_precios_path = os.path.join(GOLD_DATA_PATH, "precios_por_area")
    gold_anfitriones_path = os.path.join(GOLD_DATA_PATH, "anfitriones_clave")

    # Asegúrate de que las carpetas de salida existan
    os.makedirs(SILVER_DATA_PATH, exist_ok=True)
    os.makedirs(GOLD_DATA_PATH, exist_ok=True)
    
    raw_to_silver(csv_file_path, silver_path)
    silver_to_gold(silver_path, gold_precios_path, gold_anfitriones_path)

    spark.stop()
