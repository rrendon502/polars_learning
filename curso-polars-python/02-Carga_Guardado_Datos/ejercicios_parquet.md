# Ejercicios: Lectura y Escritura de Archivos Parquet con Polars

### Ejercicio 1: Ciclo Completo de Parquet con Datos de Sensores

**Enunciado:**
1.  Crea un DataFrame de Polars con los siguientes datos de sensores:
    *   `Timestamp`: Una secuencia de 5 datetimes comenzando desde `pl.datetime(2024, 3, 10, 12, 0, 0)` con intervalos de 30 minutos.
    *   `SensorID`: `["S001", "S002", "S001", "S003", "S002"]`
    *   `Temperatura`: `[22.5, 23.1, 22.8, None, 24.0]` (Un valor nulo)
    *   `Humedad`: `[55.0, 58.2, 56.5, 60.1, 57.9]`
    *   `Bateria_Nivel`: `[0.95, 0.80, None, 0.99, 0.75]` (Un valor nulo, tipo float)
2.  Escribe este DataFrame a un archivo Parquet llamado `datos_sensores.parquet`. Utiliza la compresión `snappy` (si está disponible, sino la compresión por defecto).
3.  Lee el archivo `datos_sensores.parquet` de nuevo en un nuevo DataFrame llamado `df_sensores_leido`.
4.  Verifica que el DataFrame leído (`df_sensores_leido`) tiene el mismo esquema (nombres de columna y tipos de datos) que el DataFrame original. Imprime el resultado de la comparación de esquemas y también el `glimpse()` de ambos DataFrames.
5.  Lee nuevamente `datos_sensores.parquet`, pero esta vez selecciona solo las columnas `Timestamp` y `Temperatura`. Imprime este DataFrame más pequeño.
6.  Limpia el archivo `datos_sensores.parquet` creado.

**Solución Propuesta:**
```python
import polars as pl
import os

# 1. Crear el DataFrame de Sensores
timestamps = pl.datetime_ranges(
    start=pl.datetime(2024, 3, 10, 12, 0, 0),
    end=pl.datetime(2024, 3, 10, 14, 0, 0), # 5 timestamps con intervalo de 30min
    interval="30m",
    eager=True
)

datos_sensores = {
    "Timestamp": timestamps,
    "SensorID": ["S001", "S002", "S001", "S003", "S002"],
    "Temperatura": [22.5, 23.1, 22.8, None, 24.0],
    "Humedad": [55.0, 58.2, 56.5, 60.1, 57.9],
    "Bateria_Nivel": [0.95, 0.80, None, 0.99, 0.75]
}
df_original_sensores = pl.DataFrame(datos_sensores)

print("DataFrame Original de Sensores:")
df_original_sensores.glimpse()

# 2. Escribir a Parquet con compresión snappy
nombre_archivo_sensores = "datos_sensores.parquet"
try:
    df_original_sensores.write_parquet(nombre_archivo_sensores, compression="snappy")
    print(f"\nDataFrame guardado en '{nombre_archivo_sensores}' con compresión snappy.")
except Exception as e:
    print(f"No se pudo guardar con 'snappy' ({e}). Guardando con compresión por defecto.")
    df_original_sensores.write_parquet(nombre_archivo_sensores)


# 3. Leer el archivo Parquet completo
df_sensores_leido = pl.read_parquet(nombre_archivo_sensores)
print("\nDataFrame de Sensores leído desde Parquet:")
df_sensores_leido.glimpse()

# 4. Verificar esquema
esquema_original = df_original_sensores.schema
esquema_leido = df_sensores_leido.schema
print(f"\n¿Esquemas iguales? {esquema_original == esquema_leido}")
# También se puede verificar con df_original_sensores.equals(df_sensores_leido)
# que es más estricto ya que también compara los datos.
print(f"¿DataFrames iguales (contenido y esquema)? {df_original_sensores.equals(df_sensores_leido)}")


# 5. Leer solo dos columnas seleccionadas
columnas_seleccionadas_sensores = ["Timestamp", "Temperatura"]
df_sensores_parcial = pl.read_parquet(nombre_archivo_sensores, columns=columnas_seleccionadas_sensores)
print(f"\nDataFrame de Sensores leído con columnas seleccionadas ({columnas_seleccionadas_sensores}):")
print(df_sensores_parcial)

# 6. Limpiar archivo
if os.path.exists(nombre_archivo_sensores):
    os.remove(nombre_archivo_sensores)
    print(f"\nArchivo '{nombre_archivo_sensores}' eliminado.")
```

### Ejercicio 2: Escritura de Datos de Eventos con Diferentes Compresiones y Lectura Selectiva

**Enunciado:**
1.  Crea un DataFrame de Polars para simular datos de eventos con las siguientes columnas:
    *   `ID_Evento`: Una secuencia de enteros de 1 a 6.
    *   `Tipo_Evento`: `["Login", "Logout", "Purchase", "ViewPage", "Error", "Login"]`
    *   `Duracion_Segundos`: `[10, 5, 120, 15, None, 8]` (Un valor nulo)
    *   `Exito`: `[True, True, True, True, False, True]`
    *   `Pais_Origen`: `["USA", "CAN", "USA", None, "MEX", "USA"]` (Un valor nulo)
2.  Escribe este DataFrame en dos archivos Parquet diferentes:
    *   `eventos_sin_compresion.parquet` sin especificar compresión (o `compression="uncompressed"`).
    *   `eventos_zstd.parquet` utilizando la compresión `zstd`.
3.  (Opcional) Imprime el tamaño de ambos archivos para observar la diferencia debida a la compresión.
4.  Lee el archivo `eventos_zstd.parquet`. Selecciona únicamente las columnas `ID_Evento` y `Duracion_Segundos`.
5.  Imprime el DataFrame resultante de la lectura selectiva.
6.  Limpia ambos archivos Parquet creados (`eventos_sin_compresion.parquet` y `eventos_zstd.parquet`).

**Solución Propuesta:**
```python
import polars as pl
import os

# 1. Crear el DataFrame de Eventos
datos_eventos = {
    "ID_Evento": [1, 2, 3, 4, 5, 6],
    "Tipo_Evento": ["Login", "Logout", "Purchase", "ViewPage", "Error", "Login"],
    "Duracion_Segundos": [10, 5, 120, 15, None, 8],
    "Exito": [True, True, True, True, False, True],
    "Pais_Origen": ["USA", "CAN", "USA", None, "MEX", "USA"]
}
df_original_eventos = pl.DataFrame(datos_eventos)

print("DataFrame Original de Eventos:")
df_original_eventos.glimpse()

# 2. Escribir a Parquet con diferentes compresiones
nombre_archivo_sin_comp = "eventos_sin_compresion.parquet"
nombre_archivo_zstd = "eventos_zstd.parquet"

df_original_eventos.write_parquet(nombre_archivo_sin_comp, compression="uncompressed")
print(f"\nDataFrame guardado en '{nombre_archivo_sin_comp}' sin compresión.")

try:
    df_original_eventos.write_parquet(nombre_archivo_zstd, compression="zstd")
    print(f"DataFrame guardado en '{nombre_archivo_zstd}' con compresión zstd.")
except Exception as e:
    print(f"No se pudo guardar con 'zstd' ({e}). Puede que la feature 'compresion-zstd' no esté activa.")
    # Si zstd no está disponible, eliminamos la referencia para no intentar leerlo/borrarlo luego
    nombre_archivo_zstd = None 

# 3. (Opcional) Comparar tamaños de archivo
if os.path.exists(nombre_archivo_sin_comp):
    size_sin_comp = os.path.getsize(nombre_archivo_sin_comp)
    print(f"Tamaño de '{nombre_archivo_sin_comp}': {size_sin_comp} bytes")

if nombre_archivo_zstd and os.path.exists(nombre_archivo_zstd):
    size_zstd = os.path.getsize(nombre_archivo_zstd)
    print(f"Tamaño de '{nombre_archivo_zstd}': {size_zstd} bytes")


# 4. Leer selectivamente desde el archivo comprimido con zstd
if nombre_archivo_zstd and os.path.exists(nombre_archivo_zstd):
    columnas_seleccionadas_eventos = ["ID_Evento", "Duracion_Segundos"]
    df_eventos_parcial = pl.read_parquet(nombre_archivo_zstd, columns=columnas_seleccionadas_eventos)
    
    # 5. Imprimir el DataFrame resultante
    print(f"\nDataFrame de Eventos leído desde '{nombre_archivo_zstd}' con columnas seleccionadas ({columnas_seleccionadas_eventos}):")
    print(df_eventos_parcial)
else:
    print(f"\nNo se pudo leer '{nombre_archivo_zstd}' porque no fue creado (posiblemente por falta de soporte zstd).")


# 6. Limpiar archivos
archivos_a_limpiar = [nombre_archivo_sin_comp]
if nombre_archivo_zstd: # Solo añadir si fue creado (o se intentó crear)
    archivos_a_limpiar.append(nombre_archivo_zstd)

for f_path in archivos_a_limpiar:
    if f_path and os.path.exists(f_path): # Chequear que f_path no sea None
        os.remove(f_path)
        print(f"\nArchivo '{f_path}' eliminado.")
```
