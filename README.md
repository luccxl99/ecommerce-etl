# 🛒 Ecommerce ETL Pipeline

Pipeline de dados end-to-end simulando um ambiente real de e-commerce, com foco em ingestão, transformação e disponibilização de dados para análise estratégica.

---

## 🎯 Objetivo

Construir um pipeline capaz de transformar dados brutos de e-commerce em informações confiáveis para tomada de decisão.

Este projeto simula um cenário real onde dados de vendas, clientes e produtos são processados para responder perguntas como:

- Quais são os produtos mais vendidos?
- Qual o ticket médio por cliente?
- Como está a performance de vendas ao longo do tempo?
- Quais categorias geram mais receita?

---

## 🏗️ Arquitetura

Fluxo atual do pipeline:
Dados brutos → Processamento (Python) → Banco de Dados → Análise

### 🔄 Evolução planejada (próximos passos)
Fonte de Dados → Airflow (Orquestração) → ETL (Python)
→ PostgreSQL (Data Warehouse) → Dashboard (BI)
→ Containerização com Docker

---

## ⚙️ Tecnologias

### 🧠 Atualmente
- Python (ETL)
- SQL
- Manipulação de dados

### 🚀 Em evolução
- PostgreSQL (armazenamento estruturado)
- Apache Airflow (orquestração de pipelines)
- Docker (containerização e reprodutibilidade)
- Ferramenta de BI (dashboard com insights)

---

## 🔄 Pipeline

### 1. Ingestão
- Leitura de dados brutos (simulando fontes reais como APIs ou arquivos)

### 2. Transformação
- Limpeza de dados
- Padronização
- Criação de métricas relevantes

### 3. Carga
- Armazenamento estruturado para análise

---

## 📊 Exemplos de análise

O pipeline permite gerar insights como:

```sql
SELECT category, SUM(revenue) AS total_revenue
FROM sales
GROUP BY category
ORDER BY total_revenue DESC;
```

🧪 Boas práticas aplicadas
Separação de etapas do pipeline
Organização modular do código
Transformações reprodutíveis
Estrutura preparada para escalabilidade

🚀 Roadmap
 Migrar banco de dados para PostgreSQL
 Orquestrar pipeline com Airflow
 Containerizar aplicação com Docker
 Criar dashboard com métricas de negócio
 Implementar logs e monitoramento
 Melhorar tratamento de erros

 📌 Status do projeto

🚧 Em desenvolvimento ativo — evoluindo para um pipeline próximo de ambiente produtivo.

💡 Sobre o projeto

Este projeto foi desenvolvido com foco em práticas reais de engenharia de dados, buscando simular desafios encontrados em ambientes de produção, como organização de pipelines, confiabilidade e escalabilidade.

👨‍💻 Autor

Lucca Moreno
Engenheiro de Dados | Backend Developer


