import pandas as pd
import logging
import sys
import os
import datetime

# --- Configuración del Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Variables de Configuración ---
# La ruta ahora apunta directamente al volumen compartido en el contenedor
SOURCE_FOLDER = "/app/data_sources"
RAW_FOLDER = "/app/raw_data"
CSV_FILE = "AB_NYC.csv"
CSV_FILE_PATH = os.path.join(SOURCE_FOLDER, CSV_FILE)
MIN_FILE_SIZE_BYTES = 100

# --- Funciones del Pipeline ---

def extraer_datos_csv(ruta_archivo):
    """
    Extrae datos de un archivo CSV y los carga en un DataFrame de pandas.
    Incluye manejo de errores para FileNotFoundError y otros.
    """
    try:
        logging.info(f"Iniciando la extracción del archivo: {ruta_archivo}")
        df = pd.read_csv(ruta_archivo)
        logging.info("Extracción exitosa. Datos cargados en un DataFrame.")
        return df
    except FileNotFoundError:
        logging.error(f"Error: El archivo no se encontró en la ruta: {ruta_archivo}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado durante la extracción: {e}")
        sys.exit(1)

def validar_calidad_datos(df):
    logging.info("Iniciando la validación de calidad de los datos.")
    if 'id' in df.columns and not df['id'].is_unique:
        logging.warning("Advertencia: Se encontraron IDs duplicados.")
    nulos = df.isnull().sum()
    if nulos.any():
        logging.warning(f"Advertencia: Se encontraron valores nulos: \n{nulos[nulos > 0].to_string()}")
    if 'price' in df.columns and (df['price'] <= 0).any():
        logging.warning("Advertencia: Se encontraron precios no positivos.")
    logging.info("Validación de calidad de datos finalizada.")
    return True

def guardar_datos_raw(df, carpeta_destino):
    try:
        os.makedirs(carpeta_destino, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"ab_nyc_raw_processed_{timestamp}.csv"
        ruta_completa = os.path.join(carpeta_destino, nombre_archivo)
        df.to_csv(ruta_completa, index=False, encoding='utf-8')
        logging.info(f"Datos guardados exitosamente en la capa Raw: {ruta_completa}")
        return ruta_completa
    except Exception as e:
        logging.error(f"Error al guardar los datos en la capa Raw: {e}")
        return None

def verificar_carga_raw(ruta_archivo, tamano_minimo):
    if os.path.exists(ruta_archivo) and os.path.getsize(ruta_archivo) > tamano_minimo:
        logging.info(f"Verificación exitosa: El archivo '{ruta_archivo}' existe y tiene el tamaño adecuado.")
        return True
    else:
        logging.error(f"Error de verificación: El archivo '{ruta_archivo}' no existe o está vacío.")
        return False

if __name__ == "__main__":
    df_raw = extraer_datos_csv(CSV_FILE_PATH)
    if df_raw is not None:
        validar_calidad_datos(df_raw)
        ruta_guardado = guardar_datos_raw(df_raw, RAW_FOLDER)
        if ruta_guardado:
            verificar_carga_raw(ruta_guardado, MIN_FILE_SIZE_BYTES)
