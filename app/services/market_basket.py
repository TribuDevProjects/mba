import logging
from typing import Callable, Optional

import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def load_data(
    product_names: list[str], query: str, db_url: str, client_logger=None, partial_match: bool = True
) -> tuple[pd.DataFrame, list[list[str]]] | tuple[None, None]:
    """
    Carga y filtra datos de transacciones.
    
    IMPORTANTE: Para búsquedas múltiples, filtra órdenes que contengan TODOS los productos buscados.
    Ejemplo: Si buscas "diablo, papas", retorna órdenes que tienen productos con "diablo" Y "papas".
    
    Args:
        product_names: Lista de nombres de productos a buscar
        query: Query SQL base para obtener datos
        db_url: URL de conexión a la BD
        client_logger: Logger opcional
        partial_match: Si True, busca coincidencias parciales case-insensitive (ej: "diablo" encuentra "Combo Diablo")
                      Si False, busca coincidencias exactas
    
    Returns:
        Tuple (DataFrame filtrado, grupos de productos encontrados) o (None, None) si no se encuentran
    """
    log = client_logger or logger
    engine = create_engine(db_url)
    df = pd.read_sql_query(query, engine)
    
    # Limpiar valores None/null en product_name
    initial_count = len(df)
    df = df[df["product_name"].notna() & (df["product_name"] != "None") & (df["product_name"] != "")]
    cleaned_count = initial_count - len(df)
    if cleaned_count > 0:
        log.info("Eliminados %d registros con product_name None/vacío", cleaned_count)

    # Búsqueda de productos (parcial o exacta)
    if partial_match:
        # Búsqueda parcial case-insensitive
        df["product_name_lower"] = df["product_name"].str.lower()
        product_groups = []  # Lista de grupos de productos encontrados
        
        # Para cada término buscado, encontrar productos que coincidan
        for name in product_names:
            name_lower = name.lower()
            matches = df[df["product_name_lower"].str.contains(name_lower, na=False)]["product_name"].unique()
            
            if len(matches) == 0:
                log.warning("No se encontraron productos con '%s'", name)
                return None, None
            
            product_groups.append(list(matches))
            log.info("'%s' coincide con %d productos: %s", name, len(matches), list(matches)[:5])
        
        # Para búsquedas múltiples: filtrar órdenes que contengan AL MENOS un producto de CADA grupo
        if len(product_names) > 1:
            log.info("Filtrando órdenes que contengan TODOS los términos buscados...")
            
            # Obtener órdenes que contienen al menos un producto de cada grupo
            valid_orders = None
            for group_idx, product_group in enumerate(product_groups):
                orders_with_group = set(df[df["product_name"].isin(product_group)]["order_id"].unique())
                
                if valid_orders is None:
                    valid_orders = orders_with_group
                else:
                    # Intersección: órdenes que tienen productos de TODOS los grupos
                    valid_orders = valid_orders.intersection(orders_with_group)
                
                log.info("  Grupo %d (%s): %d órdenes. Tras intersección: %d órdenes", 
                        group_idx + 1, product_names[group_idx], len(orders_with_group), len(valid_orders))
            
            if not valid_orders:
                log.warning("No se encontraron órdenes que contengan TODOS los productos: %s", product_names)
                return None, None
            
            filtered_df = df[df["order_id"].isin(valid_orders)]
            log.info("Encontradas %d órdenes con TODOS los productos solicitados", len(valid_orders))
        else:
            # Búsqueda de un solo producto
            all_matching = product_groups[0]
            orders_with_products = df[df["product_name"].isin(all_matching)]["order_id"]
            filtered_df = df[df["order_id"].isin(orders_with_products)]
            log.info("Encontradas %d órdenes con el producto solicitado", len(orders_with_products.unique()))
        
        filtered_df = filtered_df.drop(columns=["product_name_lower"])
        
    else:
        # Búsqueda exacta (comportamiento original)
        if not any(name in df["product_name"].values for name in product_names):
            log.warning("Productos no encontrados en la base de datos (búsqueda exacta): %s", product_names)
            return None, None
        
        product_groups = [[name] for name in product_names if name in df["product_name"].values]
        orders_with_products = df[df["product_name"].isin(product_names)]["order_id"]
        filtered_df = df[df["order_id"].isin(orders_with_products)]
        log.info("Encontradas %d órdenes con los productos solicitados", len(orders_with_products.unique()))
    
    del df
    return filtered_df, product_groups


def process_data(df: pd.DataFrame, client_logger=None) -> pd.DataFrame:
    log = client_logger or logger
    log.info("Procesando datos")
    basket = df.groupby("order_id")["product_name"].apply(list).reset_index(name="items")
    basket = basket.dropna(subset=["items"])
    return basket


def compute_rules(
    basket_df: pd.DataFrame, 
    min_support: float, 
    client_logger=None,
    search_product_groups: list[list[str]] | None = None
) -> pd.DataFrame:
    log = client_logger or logger
    te = TransactionEncoder()
    log.info("Transformando datos")
    basket_encoded = te.fit_transform(
        basket_df["items"].apply(lambda x: [str(item) for item in x])
    )

    log.info("Creando DataFrame binario")
    basket_encoded_df = pd.DataFrame(basket_encoded, columns=te.columns_)
    log.info("Dimension de basket: %s", basket_encoded_df.shape)

    log.info("Aplicando reglas de asociacion (%s)", basket_encoded_df.shape)
    frequent_itemsets = apriori(
        basket_encoded_df, min_support=min_support, use_colnames=True
    )

    log.info("Generando reglas (%s)", frequent_itemsets.shape)
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)

    log.info("Ordenando reglas")
    rules = rules.sort_values(["confidence"], ascending=[False])
    rules = rules.rename(
        columns={
            "antecedents": "lhs",
            "consequents": "rhs",
            "antecedent support": "antecedent_support",
            "consequent support": "consequent_support",
        }
    )

    rules["rhs_len"] = rules["rhs"].apply(lambda x: len(x))
    rules_rhs = rules[rules["rhs_len"] == 1]

    # Si buscamos múltiples productos, filtrar reglas donde el LHS contenga productos de los grupos buscados
    if search_product_groups and len(search_product_groups) > 1:
        log.info("Filtrando reglas que incluyan productos de los términos buscados en el LHS...")
        
        def contains_search_products(lhs_set, product_groups):
            """Verifica si el LHS contiene al menos un producto de cada grupo buscado."""
            # Aplanar todos los productos de todos los grupos
            all_search_products = set()
            for group in product_groups:
                all_search_products.update(group)
            
            # Verificar si algún producto del LHS está en los productos buscados
            return bool(lhs_set.intersection(all_search_products))
        
        rules_rhs["contains_search"] = rules_rhs["lhs"].apply(
            lambda lhs: contains_search_products(lhs, search_product_groups)
        )
        
        filtered_rules = rules_rhs[rules_rhs["contains_search"]]
        rules_rhs = filtered_rules.drop(columns=["contains_search"])
        
        log.info("Reglas filtradas: %d (que incluyen productos buscados en LHS)", len(rules_rhs))
    
    del basket_encoded, basket_encoded_df, frequent_itemsets
    return rules_rhs


def format_top_rules(rules: pd.DataFrame, top_n: int = 5) -> list[dict]:
    filtered_rules = rules[
        (rules["conviction"].notnull()) & (rules["conviction"] != float("inf"))
    ]
    result = filtered_rules.head(top_n).to_dict(orient="records")
    for row in result:
        row["lhs"] = list(row["lhs"])
        row["rhs"] = list(row["rhs"])
        row.pop("rhs_len", None)

    del rules, filtered_rules
    return result


def run_mba_pipeline(
    product_names: list[str],
    query: str,
    db_url: str,
    min_support: float,
    transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    top_n: int = 5,
    client_logger=None,
    partial_match: bool = True,
) -> list[dict] | None:
    """
    Pipeline completo de MBA:
    1. load_data: SQL fetch + filtro por producto (búsqueda parcial o exacta)
       - Para múltiples productos: filtra órdenes que contengan TODOS
    2. transform_fn: limpieza custom del cliente (opcional)
    3. process_data: agrupar en baskets
    4. compute_rules: TransactionEncoder + Apriori + association_rules
       - Para múltiples productos: prioriza reglas donde aparecen juntos en LHS
    5. format_top_rules: filtrar + serializar top N
    
    Args:
        product_names: Lista de productos a buscar. Si hay múltiples, busca órdenes con TODOS.
        partial_match: Si True, busca coincidencias parciales case-insensitive
    
    Returns:
        Lista de reglas de asociación o None si no se encuentran productos/reglas
    """
    result = load_data(product_names, query, db_url, client_logger, partial_match)
    if result[0] is None:
        return None
    
    df, product_groups = result

    if transform_fn is not None:
        df = transform_fn(df)

    basket = process_data(df, client_logger)
    rules = compute_rules(basket, min_support, client_logger, product_groups)
    
    if len(rules) == 0:
        log = client_logger or logger
        log.warning("No se generaron reglas de asociación con min_support=%.4f", min_support)
        return None
    
    return format_top_rules(rules, top_n)
