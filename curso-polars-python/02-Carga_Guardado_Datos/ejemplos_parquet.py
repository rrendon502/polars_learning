# ejemplos_parquet.py
# Ejemplos de lectura y escritura de archivos Parquet utilizando Polars.

import polars as pl
import os

def create_sample_dataframe():
    """Crea un DataFrame de Polars de ejemplo con diversos tipos de datos."""
    data = {
        "ID_Sensor": [1, 2, 3, 4, 5, 6],
        "Ubicacion": ["Norte", "Sur", "Este", "Oeste", "Centro", "Norte"],
        "Temperatura": [25.5, 28.3, 22.1, 30.5, 26.8, None], # Incluye nulo
        "Humedad": [60, 55, 70, 40, 65, 62],
        "Timestamp": pl.datetime_range(
            start=pl.datetime(2023, 7, 1, 10, 0, 0), 
            end=pl.datetime(2023, 7, 1, 10, 5, 0), 
            interval="1m", 
            eager=True
        ),
        "Activo": [True, True, False, True, True, None] # Incluye nulo
    }
    df = pl.DataFrame(data)
    print("--- DataFrame de Ejemplo Creado ---")
    print(df)
    df.glimpse()
    print("-" * 50)
    return df

def cleanup_generated_files(filenames):
    """Elimina los archivos Parquet generados."""
    print("\n--- Limpieza de archivos generados ---")
    for filename in filenames:
        if os.path.exists(filename):
            try:
                os.remove(filename)
                print(f"Archivo '{filename}' eliminado.")
            except OSError as e:
                print(f"Error al eliminar '{filename}': {e.strerror}")
        else:
            print(f"Archivo '{filename}' no encontrado.")
    print("-" * 50)

def main():
    """Función principal para ejecutar las demostraciones de Parquet."""
    
    print("Iniciando demostraciones de lectura/escritura de Parquet con Polars...")
    
    df_sample = create_sample_dataframe()
    
    files_generated = []

    # --- 1. Guardando DataFrame en sample_output.parquet (compresión snappy/default) ---
    print("\n--- 1. Guardando DataFrame en sample_output.parquet (compresión snappy) ---")
    file_path_1 = "sample_output.parquet"
    files_generated.append(file_path_1)
    
    try:
        # Snappy es común, pero puede no estar disponible en todas las compilaciones/entornos.
        df_sample.write_parquet(file_path_1, compression='snappy')
        print(f"DataFrame guardado en '{file_path_1}' con compresión 'snappy'.")
    except Exception as e:
        print(f"No se pudo guardar con 'snappy' ({e}). Guardando con compresión por defecto (usualmente zstd o lz4).")
        df_sample.write_parquet(file_path_1) # Dejar que Polars elija la compresión por defecto
        print(f"DataFrame guardado en '{file_path_1}' con compresión por defecto.")
    
    # Verificación del tamaño del archivo (opcional)
    if os.path.exists(file_path_1):
        print(f"Tamaño de '{file_path_1}': {os.path.getsize(file_path_1)} bytes.")
    print("-" * 50)

    # --- 2. Guardando DataFrame en sample_output_gzip.parquet (compresión gzip) ---
    print("\n--- 2. Guardando DataFrame en sample_output_gzip.parquet (compresión gzip) ---")
    file_path_2 = "sample_output_gzip.parquet"
    files_generated.append(file_path_2)
    
    df_sample.write_parquet(file_path_2, compression='gzip')
    print(f"DataFrame guardado en '{file_path_2}' con compresión 'gzip'.")
    
    # Verificación del tamaño del archivo (opcional)
    if os.path.exists(file_path_2):
        print(f"Tamaño de '{file_path_2}': {os.path.getsize(file_path_2)} bytes.")
    print("-" * 50)

    # --- 3. Leyendo sample_output.parquet de nuevo a un DataFrame ---
    print("\n--- 3. Leyendo el archivo Parquet completo 'sample_output.parquet' ---")
    if os.path.exists(file_path_1):
        df_read_full = pl.read_parquet(file_path_1)
        print("DataFrame leído desde 'sample_output.parquet':")
        print(df_read_full)
        print("Glimpse del DataFrame leído:")
        df_read_full.glimpse()
        
        # Verificar igualdad (teniendo en cuenta posibles diferencias en floats o nulos si se usó PyArrow)
        # Polars 'equals' es estricto.
        print(f"¿El DataFrame original y el leído son iguales? {df_sample.equals(df_read_full)}")
    else:
        print(f"No se pudo leer '{file_path_1}' porque no existe.")
    print("-" * 50)

    # --- 4. Leyendo un subconjunto de columnas de sample_output.parquet ---
    print("\n--- 4. Leyendo subconjunto de columnas ('ID_Sensor', 'Temperatura') de 'sample_output.parquet' ---")
    if os.path.exists(file_path_1):
        columns_to_select = ["ID_Sensor", "Temperatura"]
        df_read_subset = pl.read_parquet(file_path_1, columns=columns_to_select)
        print(f"DataFrame leído con columnas seleccionadas ({columns_to_select}):")
        print(df_read_subset)
        print("Glimpse del DataFrame con columnas seleccionadas:")
        df_read_subset.glimpse()
    else:
        print(f"No se pudo leer '{file_path_1}' porque no existe.")
    print("-" * 50)

    # Limpieza final de todos los archivos generados
    cleanup_generated_files(files_generated)
    
    print("\nDemostraciones de lectura/escritura de Parquet completadas.")

if __name__ == "__main__":
    main()
