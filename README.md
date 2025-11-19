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
<img width="313" height="405" alt="image" src="https://github.com/user-attachments/assets/a16b4995-b530-4348-a637-ea117d9c8e19" />


# Queries do Business Case
Notebook com as respostas
