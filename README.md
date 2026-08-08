# FEFC Data Engineering MVP

Base inicial de um projeto de Engenharia de Dados para o Fundo Especial de
Financiamento de Campanha (FEFC).

O objetivo desta etapa e criar uma casca limpa, organizada e executavel para as
proximas sprints, sem implementar ainda a regra de negocio completa.

## Descricao

Este repositorio organiza um MVP de pipeline de dados com:

- Apache Airflow para orquestracao
- Amazon S3 ou LocalStack como data lake
- arquitetura Medallion com Bronze e Silver
- PostgreSQL como Data Warehouse local na camada Gold
- Docker e Docker Compose para execucao local
- pytest para testes basicos

## Objetivo

Preparar a base tecnica para evoluir o projeto em tres sprints:

1. ingestao e armazenamento
2. tratamento e data warehouse
3. dashboard e validacao

## Arquitetura

Fluxo principal:

```text
TSE
-> Airflow
-> S3 Bronze
-> Transformacao / Qualidade
-> S3 Silver
-> PostgreSQL / Gold
-> Dashboard
```

Na pratica, o MVP usa uma abstracao para S3 e deixa a camada Gold preparada
para futuras tabelas fato, dimensoes e agregacoes.

### Medallion Architecture

#### Bronze

- preserva os dados brutos
- nao altera a origem
- organiza por ano de eleicao

Exemplo:

```text
s3://<bucket>/bronze/fundo_eleitoral/ano_eleicao=2024/raw/
```

#### Silver

- reserva a camada de limpeza e padronizacao
- conversao de tipos
- tratamento de nulos
- remocao de duplicidades
- validacoes basicas

Exemplo:

```text
s3://<bucket>/silver/fundo_eleitoral/ano_eleicao=2024/
```

#### Gold

- camada de consumo analitico
- no MVP, representada por PostgreSQL local
- preparada para modelo dimensional posterior

## Fluxo de Dados

```text
TSE
-> AIRFLOW
-> S3 BRONZE
-> TRANSFORMACAO
-> S3 SILVER
-> DATA WAREHOUSE / GOLD
-> DASHBOARD
```

## Tecnologias

- Python 3.12
- Apache Airflow 2.10.x
- boto3
- pandas
- SQLAlchemy
- psycopg
- python-dotenv
- pytest
- Docker
- Docker Compose
- LocalStack
- PostgreSQL

## Estrutura de Diretorios

```text
project/
|-- dags/
|   `-- fundo_eleitoral_pipeline.py
|-- src/
|   |-- ingestion/
|   |   `-- tse_client.py
|   |-- bronze/
|   |   `-- storage.py
|   |-- silver/
|   |   `-- transformations.py
|   |-- gold/
|   |   `-- loader.py
|   |-- quality/
|   |   `-- validations.py
|   |-- config/
|   |   `-- settings.py
|   `-- utils/
|       `-- logging.py
|-- tests/
|-- sql/
|-- dashboard/
|-- infrastructure/
|-- docker/
|-- .env.example
|-- docker-compose.yml
|-- Dockerfile
|-- requirements.txt
|-- pyproject.toml
`-- README.md
```

## Configuracao do Ambiente

1. Copie o arquivo de exemplo:

```bash
copy .env.example .env
```

2. Ajuste os valores locais se necessario.

3. Para usar LocalStack, mantenha:

```text
S3_ENDPOINT_URL=http://localstack:4566
```

4. Para usar AWS real, remova o endpoint do LocalStack e preencha as credenciais
reais no ambiente.

## Como Executar

Suba o ambiente local:

```bash
docker compose up --build
```

Isso inicia:

- PostgreSQL
- LocalStack
- Airflow webserver
- Airflow scheduler

## Como Acessar o Airflow

- URL: http://localhost:8080
- usuario padrao: `airflow`
- senha padrao: `airflow`

## Como Acessar o PostgreSQL

O Compose sobe um PostgreSQL local com dois propositos:

- database `fefc_dw` para o MVP do Data Warehouse
- database `airflow` para o metadata database do Airflow

Conexao padrao:

- host: `localhost`
- porta: `5432`
- usuario: definido em `.env`
- senha: definida em `.env`

## Como Utilizar o LocalStack

O LocalStack sobe com S3 habilitado.

Use esta configuracao quando estiver desenvolvendo localmente:

```text
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=sa-east-1
S3_BUCKET_NAME=fefc-data-lake
S3_ENDPOINT_URL=http://localstack:4566
```

Se quiser alternar para AWS real:

1. remova `S3_ENDPOINT_URL`
2. aponte as credenciais para a AWS de verdade
3. mantenha a mesma camada de abstracao em `src/bronze/storage.py`

## Como Executar os Testes

```bash
pytest
```

Se preferir usar o ambiente do projeto, instale as dependencias do `requirements.txt`
ou do `pyproject.toml`.

## O que Esta Implementado Agora

- estrutura inicial de diretorios
- definicao da fonte oficial do FEFC
- escopo do MVP limitado as tres ultimas eleicoes: 2020, 2022 e 2024
- ingestao oficial dos arquivos ZIP do TSE para Bronze
- configuracao por variaveis de ambiente
- DAG inicial do Airflow
- abstracao de S3
- camada inicial de qualidade
- camada inicial de acesso ao PostgreSQL
- testes basicos
- Docker Compose local
- documentacao inicial

## O que Ficou Como Placeholder

- transformacoes reais de Silver
- regras definitivas de qualidade
- modelo final do Data Warehouse
- dashboard
- indicadores e KPIs

## Roadmap das 3 Sprints

### Sprint 1 - Ingestao e Armazenamento

- P01 - Definir fonte e escopo dos dados
- P02 - Implementar ingestao
- P03 - Implementar armazenamento no S3
- P03 - Implementar armazenamento no S3 de forma particionada por ano de eleicao
- P04 - Implementar particionamento
- P05 - Implementar DAG no Airflow

### Sprint 2 - Tratamento e Data Warehouse

- P06 - Implementar tratamento
- P07 - Implementar qualidade dos dados
- P08 - Definir modelo analitico
- P09 - Implementar carga no Data Warehouse

### Sprint 3 - Dashboard e Validacao

- P10 - Definir indicadores e KPIs
- P11 - Desenvolver dashboard
- P12 - Implementar filtros
- P13 - Validar solucao ponta a ponta
- P14 - Documentar solucao

## Decisoes de Estrutura

Para manter o MVP simples e executavel:

- o projeto usa uma unica camada `src/` com modulos por dominio
- a camada Gold foi preparada com PostgreSQL local
- o Airflow usa um banco `airflow` separado no mesmo servidor PostgreSQL
- o S3 foi abstraido por uma classe especifica para evitar boto3 espalhado
- LocalStack e configurado para desenvolvimento local sem AWS real

## Proximos Passos da Sprint 1

1. padronizar um registro de manifest mais completo para cada execucao
2. definir o particionamento final do lake por ano de eleicao
3. preparar a separacao entre arquivos brutos e arquivos derivados
4. substituir os placeholders da DAG pelas etapas efetivas de tratamento
5. conectar os proximos passos da Sprint 1 ao arquivo bruto ja ingerido
