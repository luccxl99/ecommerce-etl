""""
Camada de transformação, limpeza, enriquecimento e métricas de negócio.
"""
import pandas as pd
from loguru import logger
from datetime import datetime

from config import MIN_ORDER_VALUE, CHURN_DAYS, TOP_PRODUCTS_LIMIT


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicatas, filtra valores inválidos e padroniza staus."""
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    df = df[df["unit_price"] > MIN_ORDER_VALUE]
    df = df[df["quantity"] > 0]
    df["status"] = df["status"].str.lower().str.strip()
    df["total_value"] = (df["quantity"] * df["unit_price"]).round(2)
    logger.info(f"clean_orders: {before:,} -> {len(df):,} linhas ({before - len(df)} removidas)")
    return df

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza campos de texto e remove duplicatas."""
    df = df.drop_duplicates(subset="customer_id")
    df["email"] = df["email"].str.lower().str.strip()
    df["name"] = df["name"].str.strip().str.title()
    df["state"] = df["state"].str.upper().str.strip()
    return df

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula margem bruta e padroniza categorias."""
    df = df.drop_duplicates(subset=["product_id"])
    df["category"]     = df["category"].str.title().str.strip()
    df["gross_margin"] = ((df["list_price"] -  df["cost_price"]) / df["list_price"]).round(4)
    return df

def enrich_orders(orders: pd.DataFrame,
                  customers: pd.DataFrame,
                  products: pd.DataFrame) -> pd.DataFrame:
    """Junta pedidos com clientes e produtos em uma tabela de fatos."""
    df = (
        orders
        .merge(customers[["customer_id", "name", "state"]],
               on="customer_id", how="left")
        .merge(products[["product_id", "category", "gross_margin"]],
               on="product_id", how="left")
    )
    df["order_year"]   = df["order_date"].dt.year
    df["order_month"]  = df["order_date"].dt.month
    df["gross_profit"] = (df["total_value"] * df["gross_margin"]).round(2)
    logger.info(f"enrich_orders: {len(df):,} linhas, {len(df.columns)} colunas")
    return df


def calc_revenue_by_period(orders: pd.DataFrame) -> pd.DataFrame:
    """Receita e ticket médio por ano/mês."""
    return (
        orders[orders["status"] != "cancelled"]
        .groupby(["order_year", "order_month"])
        .agg(
            total_revenue=("total_value", "sum"),
            total_orders=("order_id", "count"),
            avg_ticket=("total_value", "mean"),
            total_profit=("gross_profit", "sum"),
        )
        .reset_index()
        .round(2)
    )


def calc_top_products(orders: pd.DataFrame) -> pd.DataFrame:
    """Top N produtos por receita."""
    return (
        orders[orders["status"] != "cancelled"]
        .groupby(["product_id", "category"])
        .agg(
            total_revenue=("total_value", "sum"),
            units_sold=("quantity", "sum"),
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(TOP_PRODUCTS_LIMIT)
        .round(2)
    )


def calc_churn_risk(orders: pd.DataFrame,
                    customers: pd.DataFrame) -> pd.DataFrame:
    """Clientes sem compra nos últimos CHURN_DAYS dias."""
    today = pd.Timestamp.now()
    last_purchase = (
        orders[orders["status"] != "cancelled"]
        .groupby("customer_id")["order_date"]
        .max()
        .reset_index()
        .rename(columns={"order_date": "last_purchase_date"})
    )
    df = customers.merge(last_purchase, on="customer_id", how="left")
    df["days_since_purchase"] = (today - df["last_purchase_date"]).dt.days
    churn = df[df["days_since_purchase"] >= CHURN_DAYS].copy()
    logger.info(f"calc_churn_risk: {len(churn):,} clientes em risco")
    return churn[["customer_id", "name", "email", "state",
                  "last_purchase_date", "days_since_purchase"]]


def calc_orders_by_status(orders: pd.DataFrame) -> pd.DataFrame:
    """Distribuição de pedidos por status."""
    total = len(orders)
    return (
        orders.groupby("status")
        .agg(count=("order_id", "count"), revenue=("total_value", "sum"))
        .reset_index()
        .assign(pct=lambda x: (x["count"] / total * 100).round(2))
        .sort_values("count", ascending=False)
    )


def transform_all(raw: dict) -> dict:
    logger.info("=== TRANSFORM: iniciando ===")

    orders    = clean_orders(raw["orders"])
    customers = clean_customers(raw["customers"])
    products  = clean_products(raw["products"])
    fact_orders = enrich_orders(orders, customers, products)

    result = {
        "dim_customers":        customers,
        "dim_products":         products,
        "fact_orders":          fact_orders,
        "agg_revenue_period":   calc_revenue_by_period(fact_orders),
        "agg_top_products":     calc_top_products(fact_orders),
        "agg_churn_risk":       calc_churn_risk(fact_orders, customers),
        "agg_orders_by_status": calc_orders_by_status(fact_orders),
    }

    logger.success("=== TRANSFORM: concluído ===")
    return result