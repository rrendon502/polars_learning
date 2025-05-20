# Ejercicios: Módulo 1

## Introducción a Polars: Series y DataFrames

### Ejercicio 1: Creación e Inspección de una Serie de Ventas

**Enunciado:**
1.  Crea una Serie de Polars llamada `ventas_mensuales` que contenga los siguientes datos de ventas (en miles de USD): `[120, 135, 150, 140, 160, 155, 170, 165, 180, 175, 190, 200]`.
2.  Imprime la Serie.
3.  Calcula e imprime el total de ventas anuales (la suma de la Serie).
4.  Calcula e imprime el promedio de ventas mensuales.
5.  Imprime el nombre y el tipo de dato de la Serie.

**Solución Propuesta:**
```python
import polars as pl

# 1. Crear la Serie
ventas_data = [120, 135, 150, 140, 160, 155, 170, 165, 180, 175, 190, 200]
ventas_mensuales = pl.Series("ventas_mensuales", ventas_data)

# 2. Imprimir la Serie
print("Serie de Ventas Mensuales:")
print(ventas_mensuales)
print("-" * 30)

# 3. Calcular e imprimir el total de ventas anuales
total_ventas = ventas_mensuales.sum()
print(f"Total de ventas anuales (en miles de USD): {total_ventas}")
print("-" * 30)

# 4. Calcular e imprimir el promedio de ventas mensuales
promedio_ventas = ventas_mensuales.mean()
print(f"Promedio de ventas mensuales (en miles de USD): {promedio_ventas}")
print("-" * 30)

# 5. Imprimir el nombre y el tipo de dato de la Serie
print(f"Nombre de la Serie: {ventas_mensuales.name}")
print(f"Tipo de dato de la Serie: {ventas_mensuales.dtype}")
```

### Ejercicio 2: Creación e Inspección Básica de un DataFrame de Empleados

**Enunciado:**
1.  Crea un DataFrame de Polars a partir de los siguientes datos:
    *   `ID_Empleado`: `[101, 102, 103, 104, 105]`
    *   `Nombre`: `["Alice", "Bob", "Charlie", "David", "Eve"]`
    *   `Departamento`: `["Ventas", "Marketing", "TI", "Ventas", "TI"]`
    *   `Salario`: `[55000, 60000, 70000, 58000, 72000]`
2.  Imprime el DataFrame completo.
3.  Muestra las primeras 3 filas del DataFrame.
4.  Imprime los nombres de las columnas y los tipos de datos de cada columna.
5.  Imprime el número total de empleados (filas) en el DataFrame.

**Solución Propuesta:**
```python
import polars as pl

# 1. Crear el DataFrame
datos_empleados = {
    "ID_Empleado": [101, 102, 103, 104, 105],
    "Nombre": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "Departamento": ["Ventas", "Marketing", "TI", "Ventas", "TI"],
    "Salario": [55000, 60000, 70000, 58000, 72000]
}
df_empleados = pl.DataFrame(datos_empleados)

# 2. Imprimir el DataFrame completo
print("DataFrame de Empleados:")
print(df_empleados)
print("-" * 30)

# 3. Mostrar las primeras 3 filas del DataFrame
print("Primeras 3 filas del DataFrame:")
print(df_empleados.head(3))
print("-" * 30)

# 4. Imprimir los nombres de las columnas y los tipos de datos
print(f"Nombres de las columnas: {df_empleados.columns}")
print("Tipos de datos de cada columna:")
# Dtypes se muestra como una lista, se puede iterar para mejor formato si se desea
print(df_empleados.dtypes) 
# O de forma más detallada:
# for col_name, dtype in zip(df_empleados.columns, df_empleados.dtypes):
#     print(f" - Columna '{col_name}': {dtype}")
print("-" * 30)

# 5. Imprimir el número total de empleados (filas)
print(f"Número total de empleados (filas): {df_empleados.height}")
# Alternativamente, se puede usar df_empleados.shape[0]
# print(f"Número total de empleados (filas): {df_empleados.shape[0]}")
```
