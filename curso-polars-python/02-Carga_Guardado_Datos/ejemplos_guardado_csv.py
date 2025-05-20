# ejemplos_guardado_csv.py
# Ejemplos de guardado de DataFrames de Polars en archivos CSV.

import polars as pl
import os

def create_sample_dataframe():
    """Crea un DataFrame de Polars de ejemplo con diversos tipos de datos."""
    data = {
        "ID_Usuario": [101, 102, 103, 104, 105],
        "Nombre": ["Carlos", "Ana", "Pedro", "Laura", "Luis"],
        "Edad": [28, 34, None, 45, 22],  # Incluye un valor nulo
        "Puntuacion_Examen": [8.5, 9.2, 7.0, None, 6.5], # Incluye un valor nulo
        "Fecha_Registro": pl.date_range(start=pl.Date(2023, 1, 1), end=pl.Date(2023, 1, 5), interval="1d", eager=True),
        "Activo": [True, False, True, False, True]
    }
    df = pl.DataFrame(data)
    print("DataFrame de Ejemplo Creado:")
    print(df)
    print("-" * 50)
    return df

def cleanup_generated_files(filenames):
    """Elimina los archivos CSV generados."""
    print("\n--- Limpieza de archivos generados ---")
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Archivo '{filename}' eliminado.")
        else:
            print(f"Archivo '{filename}' no encontrado (posiblemente no fue creado o ya fue eliminado).")
    print("-" * 50)

def main():
    """Función principal para ejecutar las demostraciones de guardado de CSV."""
    
    print("Iniciando demostraciones de guardado de DataFrames en CSV con Polars...")
    
    df_sample = create_sample_dataframe()
    
    files_generated = []

    # --- 1. Guardado básico en output1.csv ---
    print("\n--- 1. Guardado básico en output1.csv ---")
    file1 = "output1.csv"
    files_generated.append(file1)
    
    df_sample.write_csv(file1)
    print(f"DataFrame guardado en '{file1}' con configuración por defecto.")
    
    # Verificación opcional: leer y mostrar el contenido del archivo guardado
    print("Verificación (contenido de output1.csv):")
    print(pl.read_csv(file1))
    print("-" * 50)

    # --- 2. Guardado en output2.csv con separador punto y coma (';') ---
    print("\n--- 2. Guardado en output2.csv con separador ';' ---")
    file2 = "output2.csv"
    files_generated.append(file2)
    
    df_sample.write_csv(file2, separator=';')
    print(f"DataFrame guardado en '{file2}' usando ';' como separador.")
    
    # Verificación opcional
    print("Verificación (contenido de output2.csv):")
    print(pl.read_csv(file2, separator=';'))
    print("-" * 50)

    # --- 3. Guardado en output3.csv sin la fila de encabezado ---
    print("\n--- 3. Guardado en output3.csv sin encabezado ---")
    file3 = "output3.csv"
    files_generated.append(file3)
    
    df_sample.write_csv(file3, has_header=False)
    print(f"DataFrame guardado en '{file3}' sin la fila de encabezado.")
    
    # Verificación opcional: al leer, necesitamos especificar que no hay encabezado
    # y opcionalmente podemos proveer los nombres de columna si los conocemos.
    print("Verificación (contenido de output3.csv):")
    # Para una correcta visualización, asignamos los nombres de columna originales al leerlo
    df_read_no_header = pl.read_csv(file3, has_header=False, new_columns=df_sample.columns)
    print(df_read_no_header)
    print("-" * 50)

    # --- 4. Guardado en output4.csv con una cadena personalizada para valores nulos ---
    print("\n--- 4. Guardado en output4.csv con '--NA--' para nulos ---")
    file4 = "output4.csv"
    files_generated.append(file4)
    custom_null_value = "--NA--"
    
    df_sample.write_csv(file4, null_value=custom_null_value)
    print(f"DataFrame guardado en '{file4}' usando '{custom_null_value}' para representar valores nulos.")
    
    # Verificación opcional: al leer, necesitamos especificar cómo se representaron los nulos
    print("Verificación (contenido de output4.csv y conteo de nulos):")
    df_read_custom_nulls = pl.read_csv(file4, null_values=custom_null_value)
    print(df_read_custom_nulls)
    print("Conteo de nulos en el DataFrame releído:")
    print(df_read_custom_nulls.null_count())
    print("-" * 50)

    # Limpieza final de todos los archivos generados
    cleanup_generated_files(files_generated)
    
    print("\nDemostraciones de guardado de CSV completadas.")

if __name__ == "__main__":
    main()
