"""
pipeline.py — Orquestrador principal do ETL.

Uso:
    python pipeline.py               # executa tudo
    python pipeline.py --step extract
    python pipeline.py --step transform
    python pipeline.py --step load
"""
import argparse
import sys
from loguru import logger

from etl.extract.extractor     import extract_all
from etl.transform.transformer import transform_all
from etl.load.loader           import load_all


def configure_logging():
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
        level="INFO",
        colorize=True,
    )
    logger.add(
        "pipeline.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level="DEBUG",
        rotation="10 MB",
    )


def run_pipeline(step: str = "all"):
    configure_logging()
    logger.info(f"Pipeline iniciado — step={step}")

    raw         = None
    transformed = None

    if step in ("all", "extract"):
        raw = extract_all()

    if step in ("all", "transform"):
        transformed = transform_all(raw)

    if step in ("all", "load"):
        load_all(transformed)

    logger.success("Pipeline finalizado com sucesso ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce ETL Pipeline")
    parser.add_argument(
        "--step",
        choices=["all", "extract", "transform", "load"],
        default="all",
    )
    args = parser.parse_args()
    run_pipeline(args.step)