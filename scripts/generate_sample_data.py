"""
Gera dados fictícios de e-commerce para testar o pipeline.
Produz 3 CSVs: customers.csv, products.csv, orders.csv
"""

import random
import pandas as pd
from faker import Faker
from pathlib import Path
import sys



# Permite importar o config.py da pasta raiz
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW

fake = Faker("pt_BR")
random.seed(42)

# Parâmetros
N_CUSTOMERS = 500
N_PRODUCTS  = 100
N_ORDERS    = 2000

STATES     = ["SP", "RJ", "MG", "RS", "BA", "PR", "SC", "GO", "PE", "CE"]
CATEGORIES = ["Eletrônicos", "Roupas", "Casa & Jardim", "Esportes", "Livros"]
STATUSES   = ["completed", "completed", "completed", "pending", "shipped", "cancelled"]
#                     Repetido propositalmente para ter mais pedidos completos



def gen_customers():
    rows = []
    for _ in range(N_CUSTOMERS):
        rows.append({
            "customer_id": fake.uuid4()[:8].upper(),
            "name":        fake.name(),
            "email":       fake.email(),
            "state":       random.choice(STATES),
            "created_at":  fake.date_time_between(start_date="-3y", end_date="-6m"),
        })
    return pd.DataFrame(rows)

def gen_products():
    rows = []
    for i in range(N_PRODUCTS):
        cost = round(random.uniform(5, 500), 2)
        rows.append({
            "product_id": f"PROD-{i+1:04d}",
            "name":        fake.catch_phrase(),
            "category":    random.choice(CATEGORIES),
            "cost_price":  cost,
            "list_price": round(cost * random.uniform(1.3, 3.0), 2),
        })
    return pd.DataFrame(rows)

def gen_orders(customers, products):
    cids = customers["customer_id"].tolist()
    pids = products["product_id"].tolist()
    rows = []
    for _ in range(N_ORDERS):
        pid = random.choice(pids)
        price = products.loc[products["product_id"] == pid, "list_price"].values[0]
    
    # Introduz ~2% de preços inválidos de propósito
    # para o pipeline aprender a tratar erros reais que podem ocorrer
        unit_price = price if random.random() > 0.02 else -1.0


        rows.append({
            "order_id":   fake.uuid4()[:12].upper(),
            "customer_id": random.choice(cids),
            "product_id":  pid,
            "quantity":    random.randint(1, 5),
            "unit_price":  unit_price,
            "order_date": fake.date_time_between(start_date="-1y", end_date="now"),
            "status":     random.choice(STATUSES),
       })
    return pd.DataFrame(rows)


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    print("Gerando dados de clientes...")

    customers = gen_customers()
    customers.to_csv(DATA_RAW / "customers.csv", index=False)
    print(f"  ✅ {len(customers)} clientes")

    products = gen_products()
    products.to_csv(DATA_RAW / "products.csv", index=False)
    print(f"  ✅ {len(products):,} produtos")

    orders = gen_orders(customers, products)
    orders.to_csv(DATA_RAW / "orders.csv", index=False)
    print(f"   ✅ {len(orders):,} pedidos")

    print("\nPronto! Arquivos salvos em data/raw/")

if __name__ == "__main__":
    main()