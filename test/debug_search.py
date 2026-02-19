#!/usr/bin/env python3
"""
Debug: Ver qué productos coinciden con cada término de búsqueda.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine


def get_carlsjr_engine():
    """Obtener engine de CarlsJr."""
    try:
        from prefect.variables import Variable
        credentials = Variable.get("carlsjr_warehouse")
        db_url = (
            f"mysql+mysqlconnector://"
            f"{credentials['DB_USER']}:{credentials['DB_PASS']}@"
            f"{credentials['DB_HOST']}/{credentials['DB_DATABASE']}"
        )
    except Exception:
        import os
        db_url = (
            f"mysql+mysqlconnector://"
            f"{os.getenv('CARLSJR_DB_USER', 'root')}:{os.getenv('CARLSJR_DB_PASS', '')}@"
            f"{os.getenv('CARLSJR_DB_HOST', 'localhost')}/{os.getenv('CARLSJR_DB_DATABASE', 'carlsjr_warehouse')}"
        )
    
    return create_engine(db_url)


def debug_multi_product_search(terms: list[str]):
    """
    Debugear búsqueda de múltiples productos.
    """
    engine = get_carlsjr_engine()
    
    print(f"\n{'='*70}")
    print(f"DEBUG: BÚSQUEDA MÚLTIPLE DE PRODUCTOS")
    print(f"{'='*70}\n")
    print(f"Términos buscados: {terms}\n")
    
    # Query base
    query = """
        SELECT customer_id, order_id, pd.product_name as product_name
        FROM carlsjr_warehouse.warehouse_orders AS wo
        LEFT JOIN carlsjr_warehouse.warehouse_products AS wp ON wo.upc = wp.upc
        LEFT JOIN carlsjr_warehouse.products_dimension AS pd ON wp.id = pd.id
    """
    
    df = pd.read_sql_query(query, engine)
    
    # Limpiar None
    initial = len(df)
    df = df[df["product_name"].notna() & (df["product_name"] != "None") & (df["product_name"] != "")]
    cleaned = initial - len(df)
    print(f"📊 Total registros: {len(df):,} (eliminados {cleaned:,} None)\n")
    
    # Buscar cada término
    all_matching_products = []
    all_matching_orders = set()
    
    for term in terms:
        print(f"{'─'*70}")
        print(f"🔍 Buscando: '{term}'")
        print(f"{'─'*70}")
        
        # Búsqueda parcial
        df["product_name_lower"] = df["product_name"].str.lower()
        term_lower = term.lower()
        matches = df[df["product_name_lower"].str.contains(term_lower, na=False)]
        
        unique_products = matches["product_name"].unique()
        unique_orders = matches["order_id"].nunique()
        
        if len(unique_products) == 0:
            print(f"❌ No se encontraron productos que contengan '{term}'\n")
        else:
            print(f"✅ Encontrados {len(unique_products)} productos:")
            for idx, prod in enumerate(unique_products[:10], 1):  # Solo primeros 10
                print(f"   {idx}. {prod}")
            if len(unique_products) > 10:
                print(f"   ... y {len(unique_products) - 10} más")
            
            print(f"\n📦 Órdenes con estos productos: {unique_orders:,}")
            
            all_matching_products.extend(unique_products)
            all_matching_orders.update(matches["order_id"].unique())
        
        print()
    
    # Resumen global
    print(f"{'='*70}")
    print(f"RESUMEN GLOBAL")
    print(f"{'='*70}")
    print(f"Total productos únicos encontrados: {len(all_matching_products)}")
    print(f"Total órdenes que contienen alguno: {len(all_matching_orders):,}")
    
    # Ver distribución en esas órdenes
    filtered_df = df[df["order_id"].isin(all_matching_orders)]
    product_counts = filtered_df["product_name"].value_counts().head(10)
    
    print(f"\n📈 Top 10 productos más frecuentes en esas {len(all_matching_orders):,} órdenes:")
    print(f"{'─'*70}")
    for prod, count in product_counts.items():
        pct = (count / len(all_matching_orders)) * 100
        print(f"{prod[:50]:50s} {count:5d} órdenes ({pct:.1f}%)")
    
    print(f"\n{'='*70}\n")
    
    engine.dispose()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Tomar argumentos como términos separados
        terms = sys.argv[1:]
    else:
        print("\n💡 Uso: python3 test/debug_search.py <term1> <term2> ...")
        print("   Ejemplo: python3 test/debug_search.py ice PAPAS\n")
        terms = ["ice", "PAPAS"]
    
    debug_multi_product_search(terms)
