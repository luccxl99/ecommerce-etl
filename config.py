from pathlib import Path


# Diretório raiz do projeto
BASE_DIR = Path(__file__).parent


# Pastas de dados
DATA_RAW       = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
DATA_WAREHOUSE = BASE_DIR / "data" / "warehouse"


# Banco de dados (SQLite)
DATABASE_URL = f"sqlite:///{DATA_WAREHOUSE / 'ecommerce.db'}"



# Arquivos de entrada
RAW_ORDERS     = DATA_RAW / "orders.csv"
RAW_CUSTOMERS  = DATA_RAW / "customers.csv"
RAW_PRODUCTS   = DATA_RAW / "products.csv"



# Parâmetros de processamento de dados
CHURN_DAYS         = 60     # dias sem comprar = risco de churn
TOP_PRODUCTS_LIMIT = 10
MIN_ORDER_VALUE    = 0.01
MAX_NULL_PCT       = 0.05   # filtra pedidos com valor inválido