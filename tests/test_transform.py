import pytest
import pandas as pd
from datetime import datetime, timedelta

from etl.transform.transformer import (
    clean_orders,
    clean_customers,
    clean_products,
    calc_churn_risk,
)


@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "order_id":    ["O1", "O2", "O3", "O1"],
        "customer_id": ["C1", "C2", "C3", "C1"],
        "product_id":  ["P1", "P2", "P3", "P1"],
        "quantity":    [2, 1, 3, 2],
        "unit_price":  [100.0, -5.0, 50.0, 100.0],
        "order_date":  pd.to_datetime(["2024-01-01"] * 4),
        "status":      ["COMPLETED", "completed", "cancelled", "COMPLETED"],
    })


@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id": ["C1", "C2", "C2"],
        "name":        ["JOÃO SILVA", "maria souza", "maria souza"],
        "email":       ["JOAO@GMAIL.COM", "maria@email.com", "maria@email.com"],
        "state":       ["sp", "rj", "rj"],
        "created_at":  pd.to_datetime(["2022-01-01"] * 3),
    })


@pytest.fixture
def sample_products():
    return pd.DataFrame({
        "product_id": ["P1", "P2"],
        "name":       ["Notebook", "Mouse"],
        "category":   ["ELETRÔNICOS", "eletrônicos"],
        "cost_price": [1000.0, 30.0],
        "list_price": [1500.0, 60.0],
    })


# Testes clean_orders
def test_clean_orders_remove_duplicatas(sample_orders):
    result = clean_orders(sample_orders)
    assert result["order_id"].nunique() == len(result)

def test_clean_orders_filtra_preco_invalido(sample_orders):
    result = clean_orders(sample_orders)
    assert (result["unit_price"] > 0).all()

def test_clean_orders_normaliza_status(sample_orders):
    result = clean_orders(sample_orders)
    assert result["status"].str.islower().all()

def test_clean_orders_cria_total_value(sample_orders):
    result = clean_orders(sample_orders)
    assert "total_value" in result.columns
    row = result[result["order_id"] == "O1"].iloc[0]
    assert row["total_value"] == pytest.approx(200.0)


# Testes clean_customers
def test_clean_customers_remove_duplicatas(sample_customers):
    result = clean_customers(sample_customers)
    assert result["customer_id"].nunique() == len(result)

def test_clean_customers_email_minusculo(sample_customers):
    result = clean_customers(sample_customers)
    assert result["email"].str.islower().all()

def test_clean_customers_state_maiusculo(sample_customers):
    result = clean_customers(sample_customers)
    assert result["state"].str.isupper().all()


# Testes clean_products
def test_clean_products_calcula_margem(sample_products):
    result = clean_products(sample_products)
    assert "gross_margin" in result.columns
    p1 = result[result["product_id"] == "P1"].iloc[0]
    assert p1["gross_margin"] == pytest.approx((1500 - 1000) / 1500, rel=1e-3)


# Testes calc_churn_risk
def test_churn_identifica_cliente_inativo():
    old_date = datetime.now() - timedelta(days=90)
    orders = pd.DataFrame({
        "order_id":    ["O1"],
        "customer_id": ["C1"],
        "order_date":  [old_date],
        "status":      ["completed"],
    })
    customers = pd.DataFrame({
        "customer_id": ["C1"],
        "name":        ["Ana"],
        "email":       ["ana@x.com"],
        "state":       ["SP"],
    })
    result = calc_churn_risk(orders, customers)
    assert "C1" in result["customer_id"].values

def test_churn_ignora_cliente_recente():
    recent = datetime.now() - timedelta(days=5)
    orders = pd.DataFrame({
        "order_id":    ["O1"],
        "customer_id": ["C1"],
        "order_date":  [recent],
        "status":      ["completed"],
    })
    customers = pd.DataFrame({
        "customer_id": ["C1"],
        "name":        ["Ana"],
        "email":       ["ana@x.com"],
        "state":       ["SP"],
    })
    result = calc_churn_risk(orders, customers)
    assert "C1" not in result["customer_id"].values