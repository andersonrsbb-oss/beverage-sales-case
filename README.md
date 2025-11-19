# Beverage Sales Analytics – MVP Data Engineering Solution
## beverage-sales-case

Solução completa construída para o Business Case fornecido pela empresa. Inclui ingestão, transformação, modelagem dimensional e consultas analíticas.

# Tecnologias Utilizadas
Azure Data Factory (Ingestão – CSV → Parquet)
Azure Databricks (Transformação – Bronze → Silver → Gold)
Spark / PySpark
Delta Lake (Silver e Gold)

# Objetivo
Explicar como construí uma arquitetura escalável que atende os requisitos do case.

# Arquitetura
  - Upload manual

  - ADF → Bronze (Parquet) 

  - Databricks → Silver (Delta limpo) → Gold (fato, dimensões, summary tables) → Queries analíticas

# Estrutura do Repositório
  ├── README.md
  ├── adf/
  │   ├── factory/
  │   └── linkedTemplates/
  │   └── ARMTemplateForFactory.json
  │   └── ARMTemplateParametersForFactory.json
  ├── databricks/
  │   ├── config/
  │   │   ├── mount_data_lake.py
  │   ├── notebooks/
  │   │   ├── bronze/
  │   │   ├── gold/
  │   │   └── silver/
  │   └── ddl/
  │       ├── create_database.py
  └── .gitignore


# Queries do Business Case
Notebook com as respostas
