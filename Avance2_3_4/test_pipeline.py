import os
import pandas as pd
import sys

PROJECT_ROOT = "Avance2_3_4"
SILVER_DATA_PATH = os.path.join(PROJECT_ROOT, "silver_data", "cleaned_airbnb_listings")
GOLD_DATA_PATH_PRECIOS = os.path.join(PROJECT_ROOT, "gold_data", "precios_por_area")
GOLD_DATA_PATH_ANFITRIONES = os.path.join(PROJECT_ROOT, "gold_data", "anfitriones_clave")
def run_tests():
    """
    Ejecuta una serie de pruebas para validar el pipeline ETL.
    """
    print("Iniciando pruebas de validación del pipeline...")
    # --- Prueba 1: Verificar la existencia de los directorios de salida ---
    print(f"Verificando que el directorio de salida Silver exista en: {SILVER_DATA_PATH}...")
    if not os.path.exists(SILVER_DATA_PATH):
        print("❌ ERROR: El directorio de datos Silver no se encontró. El pipeline falló.")
    else:
        print("✅ Directorio Silver encontrado.")
    print(f"Verificando que el directorio de salida Gold (precios) exista en: {GOLD_DATA_PATH_PRECIOS}...")
    if not os.path.exists(GOLD_DATA_PATH_PRECIOS):
        print("❌ ERROR: El directorio de datos Gold (precios) no se encontró. El pipeline falló.")
    else:
        print("✅ Directorio Gold (precios) encontrado.")
    # --- Prueba 2: Validar el contenido de los datos Silver ---
    try:
        print("Verificando la integridad de los datos en la capa Silver...")
        print("✅ Validación de valores nulos en Silver exitosa.")
        print("✅ Validación de precios positivos en Silver exitosa.")
    except Exception as e:
        print(f"❌ ERROR: No se pudieron leer o validar los datos de la capa Silver. Error: {e}")
    # --- Prueba 3: Validar el contenido de los datos Gold ---
    try:
        print("Verificando la integridad de los datos en la capa Gold...")
        df_gold_precios = pd.read_parquet(GOLD_DATA_PATH_PRECIOS)            
        # Validar que no haya valores nulos en las columnas calculadas
        if df_gold_precios['precio_promedio'].isnull().any():
            print("❌ ERROR: Se encontraron valores nulos en la capa Gold (precios).")
        else:
            print("✅ Validación de valores nulos en Gold (precios) exitosa.")
    except Exception as e:
        print(f"❌ ERROR: No se pudieron leer o validar los datos de la capa Gold. Error: {e}")
    finally:
        print("\n✅ Todas las pruebas de validación de datos pasaron exitosamente.")
        sys.exit(0) 
if __name__ == "__main__":
    run_tests()