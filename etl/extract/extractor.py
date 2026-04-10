"""
Camada de extração - lê arquivos CSV e valida se estão no formato esperado.
"""
import pandas as pd
from loguru import logger
from pathlib import Path

from config import RAW_ORDERS, RAW_CUSTOMERS, RAW_PRODUCTS, MAX_NULL_PCT

# Define quais colunas cada arquivo deve ter
SCHEMAS = {
    "orders":{
        "required_cols": ["order_id", "customer_id", "product_id",
                          "quantity", "unit_price", "order_date", "status"],
    },
    "customers":{
        "required_cols": ["customer_id", "name", "email",
                          "state", "created_at"],
    },
    "products":{
        "required_cols": ["product_id", "name", "category",
                          "cost_price", "list_price"],
    },
}

def _validate(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Valida se o DataFrame tem as colunas esperadas e avisa sobre nulos."""
    schema = SCHEMAS[name]

    # Checa colunas obrigatórias
    missing = set(schema["required_cols"]) - set(df.columns)
    if missing:
        raise ValueError(f"[{name}] Colunas ausentes: {missing}")

    # Avisa se alguma coluna tem excesso de nulos
    null_pct = df.isnull().mean()
    bad_cols = null_pct[null_pct > MAX_NULL_PCT]
    if not bad_cols.empty:
        logger.warning(f"[{name}] Colunas com excesso de nulos:\n{bad_cols}")

    logger.info(f"[{name}] {len(df):,} linhas extraídas — schema OK")
    return df


def _read_csv(path: Path, name: str, **kwargs) -> pd.DataFrame:
    """Lê um CSV e valida o schema."""
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path, **kwargs)
    return _validate(df, name)


def extract_orders() -> pd.DataFrame:
    return _read_csv(RAW_ORDERS, "orders",
                     dtype={"order_id": str, "customer_id": str, "product_id": str},
                     parse_dates=["order_date"])


def extract_customers() -> pd.DataFrame:
    return _read_csv(RAW_CUSTOMERS, "customers",
                     dtype={"customer_id": str},
                     parse_dates=["created_at"])


def extract_products() -> pd.DataFrame:
    return _read_csv(RAW_PRODUCTS, "products",
                     dtype={"product_id": str})


def extract_all() -> dict:
    logger.info("=== EXTRACT: iniciando ===")
    data = {
        "orders":    extract_orders(),
        "customers": extract_customers(),
        "products":  extract_products(),
    }
    logger.success("=== EXTRACT: concluído ===")
    return data