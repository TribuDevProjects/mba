import logging
from typing import Callable, Optional

import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def load_data(
    product_names: list[str], query: str, db_url: str, client_logger=None
) -> pd.DataFrame | None:
    log = client_logger or logger
    engine = create_engine(db_url)
    df = pd.read_sql_query(query, engine)

    if not any(name in df["product_name"].values for name in product_names):
        log.warning("Productos no encontrados en la base de datos: %s", product_names)
        return None

    orders_with_products = df[df["product_name"].isin(product_names)]["order_id"]
    filtered_df = df[df["order_id"].isin(orders_with_products)]
    del df
    return filtered_df


def process_data(df: pd.DataFrame, client_logger=None) -> pd.DataFrame:
    log = client_logger or logger
    log.info("Procesando datos")
    basket = df.groupby("order_id")["product_name"].apply(list).reset_index(name="items")
    basket = basket.dropna(subset=["items"])
    return basket


def compute_rules(basket_df: pd.DataFrame, min_support: float, client_logger=None) -> pd.DataFrame:
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
) -> list[dict] | None:
    """
    Pipeline completo de MBA:
    1. load_data: SQL fetch + filtro por producto
    2. transform_fn: limpieza custom del cliente (opcional)
    3. process_data: agrupar en baskets
    4. compute_rules: TransactionEncoder + Apriori + association_rules
    5. format_top_rules: filtrar + serializar top N
    """
    df = load_data(product_names, query, db_url, client_logger)
    if df is None:
        return None

    if transform_fn is not None:
        df = transform_fn(df)

    basket = process_data(df, client_logger)
    rules = compute_rules(basket, min_support, client_logger)
    return format_top_rules(rules, top_n)
