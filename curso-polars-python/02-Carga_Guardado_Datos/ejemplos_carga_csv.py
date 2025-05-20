# ejemplos_carga_csv.py
# Ejemplos de carga de archivos CSV utilizando Polars.

import polars as pl
import os

# --- Funciones para Crear y Eliminar Archivos CSV de Ejemplo ---

def create_sample_csvs():
    """Crea dos archivos CSV de ejemplo para las demostraciones."""
    
    # Contenido para sample1.csv (con encabezado, separado por comas)
    csv_content_1 = (
        "ID,Nombre,Edad,Activo\n"
        "1,Alice,30,true\n"
        "2,Bob,24,false\n"
        "3,Charlie,35,true\n"
        "4,David,28,false"
    )
    with open("sample1.csv", "w", encoding="utf-8") as f:
        f.write(csv_content_1)
    print("Archivo 'sample1.csv' creado.")

    # Contenido para sample2.csv (sin encabezado, separado por punto y coma)
    csv_content_2 = (
        "101;ProductoA;15.50;100\n"
        "102;ProductoB;25.75;50\n"
        "103;ProductoC;5.99;200\n"
        "104;ProductoD;12.00;0"
    )
    with open("sample2.csv", "w", encoding="utf-8") as f:
        f.write(csv_content_2)
    print("Archivo 'sample2.csv' creado.")
    print("-" * 40)

def remove_sample_csvs():
    """Elimina los archivos CSV de ejemplo si existen."""
    files_to_remove = ["sample1.csv", "sample2.csv"]
    for file_name in files_to_remove:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Archivo '{file_name}' eliminado.")
    print("-" * 40)

# --- Función Principal del Script ---

def main():
    """Ejecuta las demostraciones de lectura de CSV con Polars."""
    
    print("Iniciando demostraciones de carga de CSV con Polars...")
    create_sample_csvs()

    # --- Leer sample1.csv (con encabezado, separado por comas) ---
    print("\n--- Leyendo sample1.csv ---")

    # 1. Lectura básica de sample1.csv
    # Polars infiere el separador y la presencia de encabezado.
    df1_basic = pl.read_csv("sample1.csv")
    print("\n1. Lectura básica de sample1.csv:")
    print(df1_basic)
    df1_basic.glimpse()

    # 2. Leer sample1.csv con n_rows=2
    # Carga solo las primeras 2 filas de datos (más el encabezado).
    df1_nrows = pl.read_csv("sample1.csv", n_rows=2)
    print("\n2. Leer sample1.csv con n_rows=2:")
    print(df1_nrows)

    # 3. Leer sample1.csv seleccionando columnas específicas por nombre
    # Carga solo las columnas 'ID' y 'Activo'.
    df1_select_cols = pl.read_csv("sample1.csv", columns=["ID", "Activo"])
    print("\n3. Leer sample1.csv seleccionando columnas ['ID', 'Activo']:")
    print(df1_select_cols)

    # 4. Leer sample1.csv especificando dtype para una columna
    # Carga la columna 'Edad' como Float32 en lugar del Int64 inferido.
    df1_dtype = pl.read_csv("sample1.csv", dtypes={"Edad": pl.Float32})
    print("\n4. Leer sample1.csv especificando Edad como Float32:")
    print(df1_dtype)
    df1_dtype.glimpse()
    print("-" * 40)

    # --- Leer sample2.csv (sin encabezado, separado por punto y coma) ---
    print("\n--- Leyendo sample2.csv ---")

    # 1. Leer sample2.csv especificando has_header=False y separator=';'
    # Polars asignará nombres de columna genéricos (column_1, column_2, etc.)
    df2_no_header = pl.read_csv("sample2.csv", has_header=False, separator=';')
    print("\n1. Leer sample2.csv con has_header=False y separator=';':")
    print(df2_no_header)

    # 2. Leer sample2.csv proporcionando new_columns
    # Se asignan nombres personalizados a las columnas durante la carga.
    custom_column_names = ["Codigo", "Articulo", "Precio", "Stock"]
    df2_new_cols = pl.read_csv(
        "sample2.csv", 
        has_header=False, 
        separator=';', 
        new_columns=custom_column_names
    )
    print("\n2. Leer sample2.csv con new_columns:")
    print(df2_new_cols)
    df2_new_cols.glimpse()
    print("-" * 40)

    # Limpiar archivos de ejemplo
    remove_sample_csvs()
    print("Script de ejemplos de carga de CSV completado.")

if __name__ == "__main__":
    main()
