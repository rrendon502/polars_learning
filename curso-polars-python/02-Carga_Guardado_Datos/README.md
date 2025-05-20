# Módulo 2: Carga y Guardado de Datos en Polars

Este módulo se centra en una de las tareas más fundamentales en cualquier flujo de trabajo de análisis de datos: la capacidad de leer datos desde diversas fuentes y, posteriormente, guardar los resultados o datos procesados en diferentes formatos. Polars destaca por su eficiencia y flexibilidad en estas operaciones.

## Carga de Archivos CSV

Los archivos de valores separados por comas (CSV, Comma-Separated Values) son uno de los formatos más ubicuos para almacenar datos tabulares. Son archivos de texto plano donde cada línea representa una fila de datos, y las columnas dentro de esa fila están separadas por un delimitador, comúnmente una coma.

Polars proporciona la función `pl.read_csv()` que es extremadamente potente y rápida para leer este tipo de archivos, superando a menudo a otras librerías en términos de velocidad y uso de memoria.

### Objetivos de Aprendizaje (Archivos CSV)
Al finalizar esta sección, serás capaz de:
- Leer archivos CSV simples y complejos en DataFrames de Polars.
- Entender y utilizar los parámetros más comunes de la función `read_csv` para controlar el proceso de carga.
- Manejar situaciones comunes como la presencia o ausencia de encabezados, diferentes delimitadores y la selección de columnas específicas durante la carga.
- Gestionar la inferencia de tipos de datos y aprender a especificar tipos explícitamente para asegurar la correcta interpretación de los datos.
- Identificar y manejar errores básicos que pueden ocurrir durante la carga de archivos CSV.

### Temas a Cubrir (Archivos CSV)
Los temas principales para la carga de archivos CSV incluyen:
- Introducción a la función `pl.read_csv()` y sus ventajas.
- Parámetros esenciales:
    - `source`: La ruta al archivo, URL o buffer.
    - `separator` (o `sep`): Especificar el delimitador (e.g., ',', ';', '\\t').
    - `has_header`: Indicar si el archivo CSV contiene una fila de encabezado.
    - `columns`: Seleccionar un subconjunto de columnas para leer.
    - `n_rows`: Limitar el número de filas a leer (útil para inspeccionar archivos grandes).
    - `skip_rows`: Omitir un número específico de filas desde el inicio del archivo.
    - `dtypes` (o `schema`): Especificar los tipos de datos para columnas individuales o para todas las columnas.
    - `null_values`: Definir qué cadenas deben interpretarse como valores nulos.
    - `ignore_errors`: Intentar continuar la lectura a pesar de errores de parseo en algunas líneas.
    - `encoding`: Especificar la codificación del archivo (e.g., 'utf-8', 'latin1').
- Lectura de archivos CSV con y sin encabezado.
- Selección de columnas específicas durante el proceso de carga.
- Inferencia automática de esquemas (tipos de datos) por Polars y cómo anularla especificando `dtypes` o un `schema` completo.
- Lectura de porciones de archivos (`n_rows`, `skip_rows`).
- Manejo básico de errores y problemas comunes (e.g., delimitadores incorrectos, filas con errores de formato).

## Guardado de DataFrames en Archivos CSV

Una vez que hemos procesado o generado datos con Polars, a menudo necesitamos guardarlos para su uso futuro, compartirlos o para que sirvan de entrada a otros sistemas. Guardar un DataFrame en un archivo CSV es una operación común, y Polars ofrece el método `DataFrame.write_csv()` para esta tarea.

Este método permite controlar cómo se escriben los datos, incluyendo el delimitador, la inclusión de encabezados y el formato de ciertos tipos de datos.

### Objetivos de Aprendizaje (Guardado CSV)
Al finalizar esta sección, serás capaz de:
- Guardar DataFrames de Polars en archivos CSV.
- Entender y utilizar los parámetros más comunes del método `write_csv` para controlar el proceso de guardado.
- Especificar delimitadores personalizados.
- Decidir si incluir o excluir la fila de encabezado en el archivo CSV de salida.
- Comprender cómo se manejan los valores nulos y las cadenas de texto durante el guardado.

### Temas a Cubrir (Guardado CSV)
Los temas principales para el guardado de DataFrames en archivos CSV incluyen:
- Introducción al método `DataFrame.write_csv()`.
- Parámetros esenciales:
    - `file`: La ruta al archivo de salida o un buffer escribible.
    - `separator`: Especificar el delimitador para el archivo CSV de salida (por defecto, coma ',').
    - `has_header`: Booleano para incluir (por defecto `True`) o excluir la fila de encabezado.
    - `null_value`: Cadena a utilizar para representar valores nulos en el archivo CSV (por defecto, cadena vacía `""`).
- Guardar un DataFrame en un archivo CSV con la configuración por defecto.
- Especificar un separador diferente (e.g., punto y coma ';', tabulador '\\t').
- Guardar un DataFrame sin la fila de encabezado.
- Consideraciones sobre el formato de números (e.g., `float_precision`) y fechas (`date_format`, `datetime_format`) durante el guardado (breve mención, ya que Polars maneja formatos estándar por defecto de manera eficiente).

## Lectura y Escritura de Archivos Parquet

Apache Parquet es un formato de almacenamiento columnar de código abierto, optimizado para el análisis de grandes volúmenes de datos. Ofrece una compresión eficiente y un rendimiento de consulta mejorado en comparación con formatos basados en filas como CSV, especialmente para cargas de trabajo analíticas. Polars tiene un excelente soporte para Parquet, utilizando Apache Arrow para su backend.

Polars proporciona la función `pl.read_parquet()` para leer archivos Parquet y el método `DataFrame.write_parquet()` para escribir DataFrames en este formato.

### Objetivos de Aprendizaje (Parquet)
Al finalizar esta sección, serás capaz de:
- Leer datos desde archivos Parquet en DataFrames de Polars.
- Escribir DataFrames de Polars en archivos Parquet.
- Comprender las ventajas clave del formato Parquet sobre formatos como CSV para análisis de datos.
- Conocer las opciones básicas para la selección de columnas durante la lectura.
- Estar al tanto de las opciones de compresión disponibles al escribir archivos Parquet.

### Temas a Cubrir (Parquet)
Los temas principales para la lectura y escritura de archivos Parquet incluyen:
- Introducción al formato Apache Parquet y sus beneficios (almacenamiento columnar, compresión, tipos de datos enriquecidos, rendimiento).
- Lectura de archivos Parquet con `pl.read_parquet()`:
    - Lectura básica de un archivo Parquet.
    - Selección de columnas específicas durante la lectura usando el parámetro `columns`.
    - Uso de `n_rows` para leer un subconjunto de filas.
- Escritura de DataFrames en archivos Parquet con `DataFrame.write_parquet()`:
    - Escritura básica de un DataFrame.
    - Especificación de algoritmos de compresión: `compression` (e.g., 'snappy', 'gzip', 'lz4', 'zstd', 'uncompressed'). Snappy suele ser un buen equilibrio entre velocidad y ratio de compresión.
    - El parámetro `use_pyarrow` y su relevancia (Polars usa Arrow por defecto).
- Comparativa de ventajas de Parquet sobre CSV para grandes conjuntos de datos y flujos de trabajo analíticos (eficiencia de I/O, tamaño de archivo, preservación de esquemas).
