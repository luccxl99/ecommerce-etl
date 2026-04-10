"""
Camada de carga — salva os dados transformados no banco de dados.
"""
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger
from datetime import datetime

from config import DATABASE_URL, DATA_PROCESSED, DATA_WAREHOUSE


def _get_engine():
    DATA_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    return create_engine(DATABASE_URL, echo=False)


def _save_csv(df: pd.DataFrame, name: str) -> None:
    """Salva uma cópia CSV para auditoria."""
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = DATA_PROCESSED / f"{name}_{ts}.csv"
    df.to_csv(path, index=False)
    logger.debug(f"CSV salvo: {path}")


def _load_table(df: pd.DataFrame, table: str, engine,
                if_exists: str = "replace") -> None:
    """Carrega um DataFrame em uma tabela do banco."""
    df.to_sql(table, engine, if_exists=if_exists, index=False)
    logger.info(f"Tabela '{table}': {len(df):,} linhas carregadas")


def _print_summary(transformed: dict) -> None:
    """Imprime um resumo executivo no terminal."""
    rev   = transformed["agg_revenue_period"]
    churn = transformed["agg_churn_risk"]
    top   = transformed["agg_top_products"]

    total_revenue = rev["total_revenue"].sum()
    total_orders  = rev["total_orders"].sum()
    avg_ticket    = rev["avg_ticket"].mean()

    logger.info("─" * 50)
    logger.info(f"💰 Receita total:     R$ {total_revenue:>12,.2f}")
    logger.info(f"📦 Total de pedidos:  {total_orders:>12,}")
    logger.info(f"🎫 Ticket médio:      R$ {avg_ticket:>12,.2f}")
    logger.info(f"⚠️  Clientes em risco: {len(churn):>12,}")
    logger.info(f"🏆 Top produto:       {top.iloc[0]['product_id']} "
                f"(R$ {top.iloc[0]['total_revenue']:,.2f})")
    logger.info("─" * 50)


def load_all(transformed: dict) -> None:
    logger.info("=== LOAD: iniciando ===")
    engine = _get_engine()

    # Dimensões
    _load_table(transformed["dim_customers"], "dim_customers", engine)
    _load_table(transformed["dim_products"],  "dim_products",  engine)

    # Fato
    _load_table(transformed["fact_orders"], "fact_orders", engine)

    # Agregações
    for name in ["agg_revenue_period", "agg_top_products",
                 "agg_churn_risk", "agg_orders_by_status"]:
        _load_table(transformed[name], name, engine)

    # Salva CSVs de auditoria
    for name, df in transformed.items():
        _save_csv(df, name)

    _print_summary(transformed)
    logger.success("=== LOAD: concluído ===")