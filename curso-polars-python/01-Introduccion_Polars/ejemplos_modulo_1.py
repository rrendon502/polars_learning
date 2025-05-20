# Ejemplos del Módulo 1: Introducción a Polars

# 1. Importar Polars
import polars as pl
print(f"Polars version: {pl.__version__}")
print("-" * 30)

# 2. Crear una Polars Series desde una lista
# Se crea una Serie llamada 'numeros' a partir de una lista de enteros.
# Polars infiere el tipo de dato (Int64 por defecto para enteros).
series_list = pl.Series("numeros", [10, 20, 30, 40, 50])
print("Serie creada desde una lista:")
print(series_list)
print(f"Nombre de la Serie: {series_list.name}")
print(f"Tipo de dato de la Serie: {series_list.dtype}")
print("-" * 30)

# 3. Crear una Polars Series con un dtype especificado
# Se crea una Serie llamada 'precios' con tipo de dato Float64.
series_dtype = pl.Series("precios", [15.5, 25.75, 10.0, 8.99], dtype=pl.Float64)
print("Serie creada con dtype Float64:")
print(series_dtype)
print(f"Nombre de la Serie: {series_dtype.name}")
print(f"Tipo de dato de la Serie: {series_dtype.dtype}")
print("-" * 30)

# 4. Realizar una operación simple en una Series
# Calculamos la suma de la serie 'numeros' y la media de la serie 'precios'.
suma_series_list = series_list.sum()
media_series_dtype = series_dtype.mean()
print(f"Suma de la Serie 'numeros': {suma_series_list}")
print(f"Media de la Serie 'precios': {media_series_dtype}")
print("-" * 30)

# 5. Crear un Polars DataFrame desde un diccionario de listas
# Cada clave del diccionario es un nombre de columna, y cada valor es una lista de datos para esa columna.
data = {
    "ID_Producto": [1, 2, 3, 4],
    "Nombre_Producto": ["Laptop", "Mouse", "Teclado", "Monitor"],
    "Cantidad": [10, 150, 75, 25],
    "Precio_Unitario": [1200.00, 25.50, 70.00, 300.75]
}
df_dict = pl.DataFrame(data)
print("DataFrame creado desde un diccionario de listas:")
print(df_dict)
print("-" * 30)

# 6. Imprimir la forma (shape), columnas y tipos de datos (dtypes) del DataFrame
print(f"Shape del DataFrame: {df_dict.shape} (filas, columnas)")
print(f"Columnas del DataFrame: {df_dict.columns}")
print("Tipos de datos (dtypes) de las columnas:")
print(df_dict.dtypes) # Muestra una lista de dtypes
# Para una visualización más detallada de dtypes con nombres de columna:
# for col_name, dtype in zip(df_dict.columns, df_dict.dtypes):
# print(f" - Columna '{col_name}': {dtype}")
print("-" * 30)

# 7. Mostrar la cabecera (head) del DataFrame
# El método head(n) muestra las primeras n filas del DataFrame (por defecto n=5).
print("Cabecera del DataFrame (primeras 3 filas):")
print(df_dict.head(3))
print("-" * 30)

print("Fin de los ejemplos del Módulo 1.")
