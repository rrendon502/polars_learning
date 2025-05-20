# Ejercicios: Guardado de DataFrames en Archivos CSV con Polars

### Ejercicio 1: Guardar Datos de Productos con Formato Específico

**Enunciado:**
1.  Crea un DataFrame de Polars con los siguientes datos de productos:
    *   `ID_Producto`: `["P001", "P002", "P003", "P004"]`
    *   `Nombre`: `["Teclado Mecánico", "Mouse Gamer", "Monitor 27in", "Webcam HD"]`
    *   `Precio`: `[85.00, 49.99, None, 59.00]` (El precio del monitor no está disponible)
    *   `Stock`: `[150, None, 75, 120]` (El stock del mouse no está disponible)
    *   `Fecha_Lanzamiento`: `[pl.Date(2022, 10, 1), pl.Date(2023, 1, 15), pl.Date(2022, 5, 20), None]` (La fecha de la webcam no está disponible)
2.  Guarda este DataFrame en un archivo llamado `productos_exportados.csv`.
3.  Especifica que el separador debe ser un punto y coma (`;`).
4.  No incluyas la fila de encabezado en el archivo de salida.
5.  Representa todos los valores nulos (tanto de `Precio`, `Stock` como de `Fecha_Lanzamiento`) con la cadena `---`.
6.  (Opcional) Como verificación, lee el archivo `productos_exportados.csv` guardado. Muestra su contenido y sus tipos de datos. Asegúrate de usar los parámetros correctos en `pl.read_csv` para interpretarlo tal como fue guardado (sin encabezado, con el separador correcto, y manejando "---" como nulo).

**Solución Propuesta:**
```python
import polars as pl
import os

# 1. Crear el DataFrame de Productos
datos_productos = {
    "ID_Producto": ["P001", "P002", "P003", "P004"],
    "Nombre": ["Teclado Mecánico", "Mouse Gamer", "Monitor 27in", "Webcam HD"],
    "Precio": [85.00, 49.99, None, 59.00],
    "Stock": [150, None, 75, 120],
    "Fecha_Lanzamiento": [pl.Date(2022, 10, 1), pl.Date(2023, 1, 15), pl.Date(2022, 5, 20), None]
}
df_productos = pl.DataFrame(datos_productos)

print("DataFrame Original de Productos:")
print(df_productos)

# 2, 3, 4, 5. Guardar el DataFrame con las especificaciones
nombre_archivo_productos = "productos_exportados.csv"
df_productos.write_csv(
    nombre_archivo_productos,
    separator=';',
    has_header=False,
    null_value="---"
)
print(f"\nDataFrame de productos guardado en '{nombre_archivo_productos}'.")

# 6. (Opcional) Verificación
print("\nVerificación - Leyendo el archivo 'productos_exportados.csv' guardado:")
# Nombres originales de las columnas para usar al leer sin encabezado
nombres_columnas_originales = ["ID_Producto", "Nombre", "Precio", "Stock", "Fecha_Lanzamiento"]

# Al leer, es importante especificar los dtypes si la inferencia con null_values no es la deseada,
# especialmente para fechas o números que podrían ser interpretados como texto.
df_leido_productos = pl.read_csv(
    nombre_archivo_productos,
    separator=';',
    has_header=False,
    new_columns=nombres_columnas_originales,
    null_values="---",
    # Especificar dtypes es buena práctica, especialmente para columnas con nulos y formatos específicos
    dtypes={
        "Precio": pl.Float64,
        "Stock": pl.Int64, # O pl.Float64 si el stock pudiera tener decimales
        "Fecha_Lanzamiento": pl.Date
    }
)
print(df_leido_productos)
print("\nGlimpse del DataFrame leído:")
df_leido_productos.glimpse()
print("\nConteo de nulos en el DataFrame leído:")
print(df_leido_productos.null_count())


# Limpieza del archivo creado
if os.path.exists(nombre_archivo_productos):
    os.remove(nombre_archivo_productos)
    print(f"\nArchivo '{nombre_archivo_productos}' eliminado.")
```

### Ejercicio 2: Guardar Registros de Auditoría con Formato de Fecha Específico

**Enunciado:**
1.  Crea un DataFrame de Polars para simular registros de auditoría con los siguientes datos:
    *   `Timestamp`: Una secuencia de 3 datetimes, por ejemplo, comenzando desde `pl.Datetime(2023, 11, 1, 10, 0, 0)` con intervalos de 1 hora.
    *   `Usuario`: `["usuario_a", "usuario_b", "usuario_a"]`
    *   `Accion`: `["LOGIN_SUCCESS", "FILE_UPLOAD", "SETTINGS_CHANGE"]`
    *   `Exito`: `[True, True, False]`
    *   `Detalles`: `["Acceso concedido", None, "Error al aplicar cambios: Permiso denegado"]` (El detalle de la subida de archivo es nulo)
2.  Guarda este DataFrame en un archivo llamado `auditoria_exportada.csv`.
3.  Incluye la fila de encabezado.
4.  Usa el separador coma (`,`) (que es el predeterminado).
5.  Especifica que los valores nulos deben representarse con la cadena `(No Disponible)`.
6.  Formatea la columna `Timestamp` para que se guarde en el formato `YYYY-MM-DD HH:MM:SS` (Polars por defecto usa un formato ISO8601 más detallado, pero podemos especificarlo si es necesario, aunque `write_csv` suele manejar bien los datetimes estándar). *Nota: Polars `write_csv` maneja los datetimes con buena precisión por defecto. El parámetro `datetime_format` es más para `strftime` o si se necesita un formato no estándar. Para este ejercicio, observaremos el formato por defecto y discutiremos cómo `datetime_format` podría usarse.*
7.  (Opcional) Como verificación, lee el archivo guardado, muestra su contenido y verifica los tipos de datos y el formato de la columna `Timestamp`.

**Solución Propuesta:**
```python
import polars as pl
import os

# 1. Crear el DataFrame de Auditoría
# Generar Timestamps
timestamp_inicio = pl.datetime(2023, 11, 1, 10, 0, 0) # Usar pl.datetime para la creación inicial
# Polars no tiene un date_range directo para Datetime con eager=True como en versiones antiguas de pl.date_range
# Podemos crear una Serie de Timestamps de la siguiente manera:
timestamps = pl.datetime_ranges(
    start=timestamp_inicio,
    end=timestamp_inicio + pl.duration(hours=2), # 3 timestamps con intervalo de 1h
    interval="1h",
    eager=True
)


datos_auditoria = {
    "Timestamp": timestamps,
    "Usuario": ["usuario_a", "usuario_b", "usuario_a"],
    "Accion": ["LOGIN_SUCCESS", "FILE_UPLOAD", "SETTINGS_CHANGE"],
    "Exito": [True, True, False],
    "Detalles": ["Acceso concedido", None, "Error al aplicar cambios: Permiso denegado"]
}
df_auditoria = pl.DataFrame(datos_auditoria)

print("DataFrame Original de Auditoría:")
print(df_auditoria)

# 2, 3, 4, 5, 6. Guardar el DataFrame con las especificaciones
nombre_archivo_auditoria = "auditoria_exportada.csv"
df_auditoria.write_csv(
    nombre_archivo_auditoria,
    has_header=True, # Default, pero explícito para claridad
    separator=',',   # Default, pero explícito
    null_value="(No Disponible)",
    datetime_format="%Y-%m-%d %H:%M:%S" # Especificar formato para datetimes
)
print(f"\nDataFrame de auditoría guardado en '{nombre_archivo_auditoria}'.")

# 7. (Opcional) Verificación
print("\nVerificación - Leyendo el archivo 'auditoria_exportada.csv' guardado:")
df_leido_auditoria = pl.read_csv(
    nombre_archivo_auditoria,
    null_values="(No Disponible)",
    # Para parsear fechas correctamente al leer, si no se infieren bien:
    dtypes={"Timestamp": pl.Datetime} # Aseguramos que se lea como Datetime
)
print(df_leido_auditoria)
print("\nGlimpse del DataFrame leído:")
df_leido_auditoria.glimpse()

# Observar el formato de la columna Timestamp
# El formato especificado en write_csv se aplica al archivo.
# Al leerlo, Polars lo parseará a su representación interna de Datetime.
print("\nValores de Timestamp (como strings para ver formato de lectura):")
# Si quisiéramos ver el string tal cual fue leído antes de ser parseado a Datetime:
# df_leido_como_texto = pl.read_csv(nombre_archivo_auditoria, dtypes=pl.Utf8)
# print(df_leido_como_texto.get_column("Timestamp"))

print("\nConteo de nulos en el DataFrame leído:")
print(df_leido_auditoria.null_count())

# Limpieza del archivo creado
if os.path.exists(nombre_archivo_auditoria):
    os.remove(nombre_archivo_auditoria)
    print(f"\nArchivo '{nombre_archivo_auditoria}' eliminado.")

```
