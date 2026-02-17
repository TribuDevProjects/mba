#!/usr/bin/env python3
"""
Tester interactivo para probar conexiones de clientes y ejecutar queries SQL.
"""
import sys
from pathlib import Path

# Añadir el directorio raíz al path para importar los módulos de la app
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.clients import ALL_CLIENTS


def clear_screen():
    """Limpiar pantalla de consola."""
    print("\n" * 2)


def display_menu():
    """Mostrar menú de clientes disponibles."""
    print("=" * 60)
    print("TESTER DE CONEXIONES - CLIENTES DISPONIBLES")
    print("=" * 60)
    for idx, client in enumerate(ALL_CLIENTS, 1):
        print(f"{idx}. {client.CUSTOMER_NAME.upper()}")
    print(f"{len(ALL_CLIENTS) + 1}. SALIR")
    print("=" * 60)


def select_client():
    """Permitir al usuario seleccionar un cliente."""
    while True:
        try:
            choice = int(input("\nSelecciona un cliente (número): "))
            if choice == len(ALL_CLIENTS) + 1:
                print("\n👋 ¡Hasta luego!")
                sys.exit(0)
            if 1 <= choice <= len(ALL_CLIENTS):
                return ALL_CLIENTS[choice - 1]
            else:
                print(f"❌ Por favor selecciona un número entre 1 y {len(ALL_CLIENTS) + 1}")
        except ValueError:
            print("❌ Por favor ingresa un número válido")
        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            sys.exit(0)


def test_connection(client):
    """Probar conexión con el cliente seleccionado."""
    clear_screen()
    print("=" * 60)
    print(f"PROBANDO CONEXIÓN: {client.CUSTOMER_NAME.upper()}")
    print("=" * 60)
    
    try:
        print("📡 Obteniendo configuración del cliente...")
        config = client.get_config()
        
        print(f"📝 Cliente: {config.name}")
        print(f"📊 Min Support: {config.min_support}")
        print("🔐 Creando conexión a la base de datos...")
        
        engine = create_engine(config.db_url)
        
        # Test simple de conexión
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            result.fetchone()
        
        print("✅ ¡Conexión exitosa!\n")
        return engine, config
    
    except Exception as e:
        print(f"\n❌ Error al conectar: {str(e)}")
        print("\nPresiona Enter para volver al menú...")
        input()
        return None, None


def execute_query(engine, query_str):
    """Ejecutar una query SQL y mostrar resultados."""
    try:
        # Usar pandas para leer y mostrar resultados de forma legible
        df = pd.read_sql_query(query_str, engine)
        
        print("\n" + "=" * 60)
        print(f"✅ RESULTADO ({len(df)} filas)")
        print("=" * 60)
        
        if len(df) == 0:
            print("(Sin resultados)")
        else:
            # Configurar pandas para mostrar más columnas
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 50)
            
            print(df.to_string(index=False))
            print("\n" + "-" * 60)
            print(f"Tipos de datos: {dict(df.dtypes)}")
        
        print("=" * 60 + "\n")
        
    except SQLAlchemyError as e:
        print(f"\n❌ Error en la query SQL: {str(e)}\n")
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}\n")


def interactive_query_mode(engine, config):
    """Modo interactivo para ejecutar queries SQL."""
    print("\n" + "=" * 60)
    print("MODO INTERACTIVO - EJECUTOR DE QUERIES")
    print("=" * 60)
    print("Comandos disponibles:")
    print("  - Escribe una query SQL para ejecutarla")
    print("  - 'default' o 'd' - Ejecutar la query por defecto del cliente")
    print("  - 'tables' o 't' - Listar tablas disponibles")
    print("  - 'exit' o 'q' - Volver al menú principal")
    print("=" * 60 + "\n")
    
    while True:
        try:
            query_input = input("SQL> ").strip()
            
            if not query_input:
                continue
            
            # Comando para salir
            if query_input.lower() in ['exit', 'quit', 'q']:
                print("\n↩️  Volviendo al menú principal...\n")
                break
            
            # Comando para ejecutar query por defecto
            if query_input.lower() in ['default', 'd']:
                print(f"\n📋 Ejecutando query por defecto:\n{config.query[:200]}...\n")
                execute_query(engine, config.query)
                continue
            
            # Comando para listar tablas
            if query_input.lower() in ['tables', 't']:
                # Query genérica para MySQL
                tables_query = "SHOW TABLES"
                print(f"\n📋 Listando tablas disponibles:\n")
                execute_query(engine, tables_query)
                continue
            
            # Ejecutar query personalizada
            execute_query(engine, query_input)
            
        except KeyboardInterrupt:
            print("\n\n↩️  Volviendo al menú principal...\n")
            break
        except EOFError:
            print("\n\n↩️  Volviendo al menú principal...\n")
            break


def main():
    """Función principal del tester."""
    print("\n👋 Bienvenido al Tester de Conexiones de Clientes\n")
    
    if not ALL_CLIENTS:
        print("❌ No se encontraron clientes disponibles")
        sys.exit(1)
    
    while True:
        try:
            # Mostrar menú y seleccionar cliente
            display_menu()
            selected_client = select_client()
            
            # Probar conexión
            engine, config = test_connection(selected_client)
            
            if engine and config:
                # Entrar en modo interactivo
                interactive_query_mode(engine, config)
                engine.dispose()
            
        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            sys.exit(0)
        except Exception as e:
            print(f"\n❌ Error inesperado: {str(e)}")
            print("\nPresiona Enter para continuar...")
            input()


if __name__ == "__main__":
    main()
