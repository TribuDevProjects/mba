# Tester de Conexiones de Clientes

Herramienta interactiva en consola para probar las conexiones de los clientes y ejecutar queries SQL.

## Características

- **Detección automática de clientes**: Descubre dinámicamente todos los clientes disponibles en `app/clients/`
- **Conexión a bases de datos**: Establece conexión con el cliente seleccionado
- **Ejecución de queries**: Permite escribir y ejecutar queries SQL personalizadas
- **Comandos útiles**: 
  - Ejecutar query por defecto del cliente
  - Listar tablas disponibles
  - Visualización de resultados en formato de tabla

## Uso

### Ejecutar el tester

```bash
python test/tester.py
```

O si lo hiciste ejecutable:

```bash
./test/tester.py
```

### Ejemplo de sesión

1. Al iniciar, verás un menú con todos los clientes disponibles:
   ```
   ============================================================
   TESTER DE CONEXIONES - CLIENTES DISPONIBLES
   ============================================================
   1. CARLSJR
   2. MULTICARNES
   3. KARZO
   4. CYPRESS
   5. SALIR
   ============================================================
   ```

2. Selecciona un cliente escribiendo su número

3. El sistema establecerá la conexión y mostrará el modo interactivo:
   ```
   ============================================================
   MODO INTERACTIVO - EJECUTOR DE QUERIES
   ============================================================
   Comandos disponibles:
     - Escribe una query SQL para ejecutarla
     - 'default' o 'd' - Ejecutar la query por defecto del cliente
     - 'tables' o 't' - Listar tablas disponibles
     - 'exit' o 'q' - Volver al menú principal
   ============================================================
   ```

4. Escribe tus queries SQL:
   ```sql
   SQL> SELECT * FROM products LIMIT 10
   SQL> SHOW TABLES
   SQL> default
   SQL> exit
   ```

## Comandos disponibles

- **Query SQL personalizada**: Escribe cualquier query SQL válida
- **`default` o `d`**: Ejecuta la query por defecto configurada para el cliente
- **`tables` o `t`**: Lista todas las tablas disponibles en la base de datos
- **`exit`, `quit` o `q`**: Vuelve al menú principal de selección de clientes

## Requisitos

- Python 3.10+
- Dependencias instaladas (sqlalchemy, pandas, etc.)
- Variables de entorno configuradas para cada cliente (Prefect Variables)

## Notas

- El tester carga dinámicamente todos los clientes definidos en `app.clients.ALL_CLIENTS`
- Los resultados se muestran en formato de tabla con información de tipos de datos
- Se maneja automáticamente la conexión y desconexión de la base de datos
- Ctrl+C en cualquier momento para salir limpiamente
