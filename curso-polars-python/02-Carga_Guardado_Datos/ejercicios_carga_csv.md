# Ejercicios: Carga de Archivos CSV con Polars

### Ejercicio 1: Lectura de CSV de Ventas con Múltiples Opciones

**Enunciado:**
Supongamos que tienes los siguientes datos en un string, simulando un archivo CSV de ventas diarias. Los datos están separados por tabuladores (`\\t`) y no tienen una fila de encabezado.

```
2023-01-15	PROD004	CategoriaA	75.50	5
2023-01-15	PROD001	CategoriaB	120.00	2
2023-01-16	PROD004	CategoriaA	78.00	3
2023-01-16	PROD002	CategoriaC	30.25	10
2023-01-17	PROD001	CategoriaB	115.50	1
```

1.  Lee estos datos en un DataFrame de Polars.
2.  Especifica que el separador es un tabulador.
3.  Indica que el archivo no tiene encabezado.
4.  Asigna los siguientes nombres a las columnas en el orden en que aparecen: `Fecha`, `ID_Producto`, `Categoria`, `Precio_Venta`, `Unidades_Vendidas`.
5.  Una vez cargado con los nombres nuevos, selecciona solo las columnas `Fecha`, `ID_Producto` y `Precio_Venta` para el DataFrame final.
6.  Especifica los siguientes tipos de datos durante la carga para las columnas originales (antes de la selección final):
    *   `Precio_Venta` debe ser `pl.Float64`.
    *   `Unidades_Vendidas` debe ser `pl.Int16`.
    *   `Fecha` debe ser `pl.Date` (Polars intentará parsearlo, asegúrate que el formato sea compatible o considera leer como `pl.Utf8` primero si hay problemas de parseo y luego convertir).
7.  Imprime el DataFrame resultante y su información con `glimpse()`.

**Solución Propuesta:**
```python
import polars as pl
from io import StringIO

# Datos de ejemplo en un string multilínea
csv_data_ventas = """2023-01-15\tPROD004\tCategoriaA\t75.50\t5
2023-01-15\tPROD001\tCategoriaB\t120.00\t2
2023-01-16\tPROD004\tCategoriaA\t78.00\t3
2023-01-16\tPROD002\tCategoriaC\t30.25\t10
2023-01-17\tPROD001\tCategoriaB\t115.50\t1"""

# Nombres para las columnas originales
nombres_columnas_originales = ["Fecha", "ID_Producto", "Categoria", "Precio_Venta", "Unidades_Vendidas"]

# Tipos de datos para las columnas originales
tipos_datos_originales = {
    "Fecha": pl.Date, # Polars intentará parsear formatos comunes de fecha
    "Precio_Venta": pl.Float64,
    "Unidades_Vendidas": pl.Int16
}

# Columnas a seleccionar para el DataFrame final
columnas_a_seleccionar = ["Fecha", "ID_Producto", "Precio_Venta"]

# 1, 2, 3, 4, 6: Leer CSV especificando separador, sin encabezado, nombres de columna y dtypes
df_ventas_completo = pl.read_csv(
    StringIO(csv_data_ventas),
    separator='\\t',
    has_header=False,
    new_columns=nombres_columnas_originales,
    dtypes=tipos_datos_originales
)

# 5: Seleccionar un subconjunto de columnas
df_ventas_final = df_ventas_completo.select(columnas_a_seleccionar)

# 7: Imprimir el DataFrame resultante y su glimpse
print("DataFrame de Ventas Final:")
print(df_ventas_final)
print("\\nInformación del DataFrame con glimpse():")
df_ventas_final.glimpse()

# Verificación de los dtypes del DataFrame completo antes de la selección (opcional)
# print("\\nInformación del DataFrame completo (antes de seleccionar columnas) con glimpse():")
# df_ventas_completo.glimpse()
```

### Ejercicio 2: Lectura Parcial y Manejo de Nulos en CSV de Registros de Sensores

**Enunciado:**
Tienes el siguiente registro de datos de sensores, donde algunos valores faltantes están representados por "ND" (No Disponible) o simplemente están vacíos.

```csv
Timestamp,SensorID,Temperatura,Humedad,CalidadAire
2023-10-01T10:00:00,S01,25.5,60,Optima
2023-10-01T10:00:05,S02,ND,62,Buena
2023-10-01T10:00:10,S01,,58,Optima
2023-10-01T10:00:15,S03,22.1,ND,Deficiente
2023-10-01T10:00:20,S01,25.6,61,
2023-10-01T10:00:25,S02,23.0,63,Buena
2023-10-01T10:00:30,S04,28.0,55,Optima
```

1.  Lee estos datos en un DataFrame de Polars.
2.  Lee solo las primeras 5 filas de datos (sin contar la línea de encabezado).
3.  Especifica que los valores "ND" y las cadenas vacías (`""`) deben ser interpretados como valores nulos para todas las columnas.
4.  Selecciona únicamente las columnas `Timestamp`, `SensorID` y `Temperatura`.
5.  Asegúrate de que la columna `Temperatura` se lea como `pl.Float32`.
6.  Imprime el DataFrame resultante y la cantidad de valores nulos por columna en el DataFrame resultante.

**Solución Propuesta:**
```python
import polars as pl
from io import StringIO

# Datos de ejemplo
csv_data_sensores = """Timestamp,SensorID,Temperatura,Humedad,CalidadAire
2023-10-01T10:00:00,S01,25.5,60,Optima
2023-10-01T10:00:05,S02,ND,62,Buena
2023-10-01T10:00:10,S01,,58,Optima
2023-10-01T10:00:15,S03,22.1,ND,Deficiente
2023-10-01T10:00:20,S01,25.6,61,
2023-10-01T10:00:25,S02,23.0,63,Buena
2023-10-01T10:00:30,S04,28.0,55,Optima"""

# 1, 2, 3, 4, 5: Leer CSV con las especificaciones
df_sensores = pl.read_csv(
    StringIO(csv_data_sensores),
    n_rows=5,  # Leer solo las primeras 5 filas de datos
    null_values=["ND", ""], # Especificar valores nulos
    columns=["Timestamp", "SensorID", "Temperatura"], # Seleccionar columnas
    dtypes={"Temperatura": pl.Float32} # Especificar tipo para Temperatura
)

# 6. Imprimir el DataFrame resultante y su conteo de nulos
print("DataFrame de Sensores (primeras 5 filas, columnas seleccionadas, nulos procesados):")
print(df_sensores)
print("\\nConteo de valores nulos por columna:")
print(df_sensores.null_count())
print("\\nInformación del DataFrame con glimpse():")
df_sensores.glimpse()
```
