#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.market_basket import run_mba_pipeline

def get_config():
    try:
        from prefect.variables import Variable
        credentials = Variable.get("carlsjr_warehouse")
        db_url = f"mysql+mysqlconnector://{credentials['DB_USER']}:{credentials['DB_PASS']}@{credentials['DB_HOST']}/{credentials['DB_DATABASE']}"
    except:
        import os
        db_url = f"mysql+mysqlconnector://{os.getenv('CARLSJR_DB_USER')}:{os.getenv('CARLSJR_DB_PASS')}@{os.getenv('CARLSJR_DB_HOST')}/{os.getenv('CARLSJR_DB_DATABASE')}"
    
    return {
        "db_url": db_url,
        "query": "SELECT customer_id, order_id, pd.product_name as product_name FROM carlsjr_warehouse.warehouse_orders AS wo LEFT JOIN carlsjr_warehouse.warehouse_products AS wp ON wo.upc = wp.upc LEFT JOIN carlsjr_warehouse.products_dimension AS pd ON wp.id = pd.id",
        "min_support": 0.01
    }

config = get_config()

# Test 1: diablo + papas
print("\n" + "="*70)
print("🔍 TEST 1: diablo + papas (órdenes que tienen AMBOS)")
print("="*70)
rules1 = run_mba_pipeline(['diablo', 'papas'], config['query'], config['db_url'], config['min_support'], partial_match=True)

if rules1:
    print(f'\n✅ {len(rules1)} reglas encontradas:\n')
    for i, r in enumerate(rules1[:3], 1):
        print(f'{i}. {list(r["lhs"])} → {list(r["rhs"])}')
        print(f'   Confianza: {r["confidence"]:.2%}, Lift: {r["lift"]:.2f}\n')
else:
    print('\n❌ No se encontraron reglas')

# Test 2: burger + papas (debería tener más órdenes)
print("\n" + "="*70)
print("🔍 TEST 2: burger + papas (combo más común)")
print("="*70)
rules2 = run_mba_pipeline(['burger', 'papas'], config['query'], config['db_url'], config['min_support'], partial_match=True)

if rules2:
    print(f'\n✅ {len(rules2)} reglas encontradas:\n')
    for i, r in enumerate(rules2[:3], 1):
        print(f'{i}. {list(r["lhs"])} → {list(r["rhs"])}')
        print(f'   Confianza: {r["confidence"]:.2%}, Lift: {r["lift"]:.2f}\n')
else:
    print('\n❌ No se encontraron reglas')

print("="*70)
