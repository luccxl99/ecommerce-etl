from pathlib import Path

BASE_DIR = Path(__file__).parent

DATA_RAW       = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
DATA_WAREHOUSE = BASE_DIR / "data" / "warehouse"

DATABASE_URL = f"sqlite:///{DATA_WAREHOUSE / 'ecommerce.db'}"

RAW_ORDERS    = DATA_RAW / "orders.csv"
RAW_CUSTOMERS = DATA_RAW / "customers.csv"
RAW_PRODUCTS  = DATA_RAW / "products.csv"

CHURN_DAYS         = 60
TOP_PRODUCTS_LIMIT = 10
MIN_ORDER_VALUE    = 0.01
MAX_NULL_PCT       = 0.05