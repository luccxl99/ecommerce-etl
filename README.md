# 🛒 E-commerce ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?logo=pandas)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-red)
![pytest](https://img.shields.io/badge/tests-10%20passed-brightgreen?logo=pytest)
![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)

Pipeline ETL completo para processamento de dados de e-commerce, construído do zero com Python, pandas e SQLAlchemy. Processa pedidos, clientes e produtos — limpando, enriquecendo e carregando os dados em um Data Warehouse com métricas de negócio prontas para análise.

---

## 📐 Arquitetura

```
Fontes Raw (CSV)
      │
      ▼
  [EXTRACT]  ── Leitura + validação de schema
      │
      ▼
 [TRANSFORM] ── Limpeza, enriquecimento, métricas de negócio
      │
      ▼
   [LOAD]    ── Carga no Data Warehouse (SQLite/PostgreSQL)
      │
      ▼
  Relatórios & Métricas
```
---

## 🗂️ Estrutura do Projeto
```
ecommerce-etl/
├── data/
│   ├── raw/          # Dados brutos (fonte)
│   ├── processed/    # CSVs de auditoria gerados pelo pipeline
│   └── warehouse/    # Banco de dados local
├── etl/
│   ├── extract/      # Leitura e validação dos CSVs
│   ├── transform/    # Limpeza, enriquecimento e métricas
│   └── load/         # Carga no banco de dados
├── tests/            # Testes unitários com pytest
├── scripts/          # Gerador de dados de exemplo
├── config.py         # Configurações centralizadas
├── pipeline.py       # Orquestrador principal
└── requirements.txt
```
---

## 🚀 Como executar

### 1. Clonar o repositório
```bash
git clone https://github.com/luccxl99/ecommerce-etl.git
cd ecommerce-etl
```

### 2. Criar e ativar o ambiente virtual
```bash
python -m venv .venv

# Windows
.venv\Scripts\Activate.ps1

# Linux/Mac
source .venv/bin/activate
```

### 3. Instalar dependências
```bash
pip install -r requirements.txt
```

### 4. Gerar dados de exemplo
```bash
python scripts/generate_sample_data.py
```

### 5. Executar o pipeline completo
```bash
python pipeline.py
```

### 6. Executar etapas individualmente
```bash
python pipeline.py --step extract
python pipeline.py --step transform
python pipeline.py --step load
```

### 7. Rodar os testes
```bash
pytest tests/ -v
```

---

## 📊 Métricas geradas

| Tabela | Descrição |
|--------|-----------|
| `fact_orders` | Tabela de fatos com todos os pedidos enriquecidos |
| `dim_customers` | Dimensão de clientes padronizada |
| `dim_products` | Dimensão de produtos com margem bruta calculada |
| `agg_revenue_period` | Receita e ticket médio por mês/ano |
| `agg_top_products` | Top 10 produtos por receita |
| `agg_churn_risk` | Clientes sem compra há 60+ dias |
| `agg_orders_by_status` | Distribuição percentual por status |

---

## 🧪 Testes

```bash
pytest tests/ -v
```
tests/test_transform.py::test_clean_orders_remove_duplicatas PASSED
tests/test_transform.py::test_clean_orders_filtra_preco_invalido PASSED
tests/test_transform.py::test_clean_orders_normaliza_status PASSED
tests/test_transform.py::test_clean_orders_cria_total_value PASSED
tests/test_transform.py::test_clean_customers_remove_duplicatas PASSED
tests/test_transform.py::test_clean_customers_email_minusculo PASSED
tests/test_transform.py::test_clean_customers_state_maiusculo PASSED
tests/test_transform.py::test_clean_products_calcula_margem PASSED
tests/test_transform.py::test_churn_identifica_cliente_inativo PASSED
tests/test_transform.py::test_churn_ignora_cliente_recente PASSED
10 passed in 0.64s
---

## 🔧 Configuração

Edite o `config.py` para ajustar:

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `DATABASE_URL` | SQLite local | Troque por PostgreSQL em produção |
| `CHURN_DAYS` | 60 | Dias sem compra para risco de churn |
| `TOP_PRODUCTS_LIMIT` | 10 | Quantidade no ranking de produtos |
| `MIN_ORDER_VALUE` | 0.01 | Valor mínimo válido por pedido |
| `MAX_NULL_PCT` | 0.05 | Tolerância de nulos por coluna (5%) |

---

## 🛣️ Roadmap

- [x] Pipeline ETL completo (Extract → Transform → Load)
- [x] Validação de schema na extração
- [x] Testes unitários com pytest
- [x] Logging estruturado com loguru
- [ ] PostgreSQL + Docker
- [ ] Orquestração com Apache Airflow
- [ ] Dashboard com Streamlit

---

## 🧱 Tecnologias

| Tecnologia | Uso |
|-----------|-----|
| Python 3.12 | Linguagem principal |
| pandas | Transformações e agregações |
| SQLAlchemy | Abstração do banco de dados |
| pytest | Testes unitários |
| loguru | Logging estruturado |
| Faker | Geração de dados de teste |
