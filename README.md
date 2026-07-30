# Retail Sales Lakehouse - Databricks & PySpark

## Projeto

Este projeto implementa uma arquitetura **Lakehouse** utilizando **Databricks**, **PySpark** e **Delta Lake**, seguindo o padrão **Medallion Architecture (Bronze → Silver → Gold)**.

O objetivo foi simular um pipeline de Engenharia de Dados semelhante ao encontrado em ambientes corporativos, desde a ingestão de dados brutos até a criação de tabelas analíticas para consumo pelo negócio.

O projeto foi desenvolvido como parte da minha jornada de aprendizado em Engenharia de Dados, buscando aplicar boas práticas de organização, monitoramento da pipeline, validação de schema e processamento incremental (UPSERT) utilizando Delta Lake MERGE.

---

# Arquitetura

![Arquitetura Medallion](https://ilegra-site-cms-strapi.s3.us-east-1.amazonaws.com/medal_8c7653c3ab.png)

```
CSV
 │
 ▼
Bronze (Raw Data)
 │
 ▼
Silver (Clean Data)
 │
 ▼
Gold (Business Rules)
 │
 ▼
Dashboard / Analytics
```

---

# Tecnologias utilizadas

* Python
* PySpark
* Databricks
* Delta Lake
* Delta Merge (UPSERT)
* Delta Tables
* Apache Spark
* Unity Catalog

---

# Estrutura do projeto

```
Retail-Sales-Lakehouse/

│
├── data/
│   └── sales_data_example.csv
│
├── notebooks/
│   ├── 01_ingestao_bronze.ipynb
│   ├── 02_transformacao_silver.ipynb
│   └── 03_dados_prontos_gold.ipynb
│
├── pipeline/
│   ├── bronze.py
│   ├── bronze_utils.py
│   │
│   ├── silver.py
│   ├── silver_utils.py
│   │
│   ├── gold.py
│   └── gold_utils.py
│
├── monitoramento/
│   └── monitoramento.py
│
└── README.md
```

> **Nota de Desenvolvimento:** O fluxo de desenvolvimento começa na pasta `notebooks/`, onde cada bloco de código e lógica é testado e validado interativamente. Após a validação, a lógica é modularizada e migrada para arquivos estruturados `.py` dentro da pasta `pipeline/` para execução em produção.

---

# Dataset

O diretório **data/** contém um arquivo de exemplo utilizado para demonstração do projeto.

```
data/
    sales_data_example.csv
```

O pipeline foi desenvolvido para ler esse arquivo, realizar todo o processamento e armazenar os resultados em tabelas Delta dentro do Databricks.

---

# Camada Bronze

A camada Bronze possui apenas uma responsabilidade:

**Preservar os dados exatamente como chegaram na origem.**

Nenhuma transformação é realizada nessa etapa.

Fluxo:

* Leitura do CSV.
* Inferência automática do schema.
* Criação da tabela Delta.
* Armazenamento dos dados brutos.

Objetivo:

Sempre manter uma cópia original dos dados para possibilitar reprocessamentos futuros sem depender novamente da origem.

> **Adicionar screenshot da tabela Bronze aqui**

---

# Camada Silver

A camada Silver é responsável pela limpeza e padronização dos dados.

Nesta etapa todas as transformações são realizadas antes que os dados sejam disponibilizados para consumo.

## Correção do Schema

O campo **ORDERDATE** foi convertido para o tipo Date.

A partir dele foram recalculados:

* YEAR_ID
* MONTH_ID

garantindo consistência nas informações temporais.

---

## Tratamento de valores nulos

Nesta versão do projeto foi adotada a seguinte regra:

* ORDERNUMBER não pode ser nulo.

Caso um registro não possua identificador de pedido ele é removido da tabela.

---

## Remoção de duplicados

Foram removidos registros duplicados utilizando como chave:

* ORDERNUMBER
* ORDERLINENUMBER

Cada linha do pedido deve existir apenas uma vez.

---

## Padronização

Os status dos pedidos foram traduzidos para português.

Exemplo:

```
SHIPPED

↓

ENVIADO
```

Isso torna a leitura das tabelas Gold muito mais intuitiva.

---

## Derivação de campos

Foi criada a coluna:

```
vendas_calculado
```

utilizando:

```
QUANTITYORDERED × PRICEEACH
```

### Por que criar essa coluna?

Durante a análise dos dados foi observado que diversos registros apresentavam diferenças entre:

```
SALES
```

e

```
QUANTITYORDERED × PRICEEACH
```

Exemplo:

```
Quantidade: 23

Preço Unitário: 100

SALES: 2597.39

23 × 100 = 2300
```

Como não existia documentação informando descontos, impostos ou outras regras de cálculo, optei por **não confiar no campo SALES** para as análises desenvolvidas neste projeto.

Assim foi criada uma nova coluna chamada:

```
vendas_calculado
```

garantindo que todas as métricas da camada Gold utilizassem um cálculo consistente.

---

## Renomeação das colunas

As colunas foram renomeadas para um padrão mais legível.

Exemplo:

```
ORDERNUMBER

↓

numero_pedido
```

```
PRICEEACH

↓

preco_unitario
```

```
SALES

↓

vendas
```

---

## Escrita da Silver

Caso a tabela ainda não exista:

* cria automaticamente a tabela Delta.

Caso ela já exista:

* valida o schema;
* verifica alterações;
* executa um MERGE (UPSERT);
* atualiza registros existentes;
* insere novos registros.

Esse comportamento evita duplicação de dados e permite cargas incrementais.

> **Adicionar screenshot do MERGE ou da tabela Silver aqui**

---

# Camada Gold

A camada Gold contém apenas informações voltadas ao negócio.

Todas as regras são derivadas da tabela:

```
clean_sales
```

---

## Regra de negócio 1

### Vendas por período

Agrupa os pedidos por:

* Ano

Calcula:

* Valor total vendido
* Quantidade de produtos vendidos
* Quantidade de linhas dos pedidos

---

## Regra de negócio 2

### Vendas por status

Calcula:

* Total vendido
* Quantidade vendida
* Total de pedidos

Agrupando por:

* ENVIADO
* CANCELADO
* RESOLVIDO
* EM PROCESSAMENTO
* EM ESPERA
* EM DISPUTA

---

## Regra de negócio 3

### Top vendas

Identifica os pedidos com maior faturamento considerando:

* Ano
* Mês

Calculando:

* Valor total
* Quantidade vendida
* Número de itens do pedido

---

> **Adicionar screenshot das tabelas Gold aqui**

---

# Monitoramento

Durante toda a execução do pipeline são gerados logs informativos.

Exemplo:

```
[INFO] Iniciando camada Bronze

[INFO] Arquivo CSV carregado

[INFO] Tabela Delta criada

[INFO] Camada Bronze finalizada em 4.77 segundos
```

Também são registrados:

* erros;
* alterações de schema;
* tempo de execução;
* progresso das etapas.

> **Adicionar screenshot dos logs aqui**

---

# Fluxo completo

```
CSV

↓

Bronze

↓

Silver

↓

Validação

↓

Merge

↓

Gold

↓

Tabelas Analíticas
```

---

# O que aprendi

Durante este projeto tive contato com diversos conceitos utilizados em Engenharia de Dados:

* Arquitetura Medallion
* Apache Spark
* PySpark
* Delta Lake
* Delta Merge (UPSERT)
* Delta Tables
* Processamento incremental
* Tratamento de dados
* Padronização de datasets
* Evolução de schema
* Modularização utilizando Python
* Monitoramento de pipelines
* Criação de métricas de negócio

---

# Próximos passos

* Implementar Airflow para orquestração.
* Automatizar execuções agendadas.
* Adicionar testes unitários.
* Implementar monitoramento persistindo logs em tabelas Delta.
* Criar dashboards consumindo as tabelas Gold.

---

## Autor

**Benilson Benito**

Junior Data Engineer

Python • SQL • PySpark • Databricks • Delta Lake • ETL/ELT
