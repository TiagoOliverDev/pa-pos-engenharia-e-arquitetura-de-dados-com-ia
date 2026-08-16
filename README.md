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
- organiza por ano de eleicao, com particionamento explicito em `ano_eleicao`

Exemplo:

```text
s3://<bucket>/bronze/fundo_eleitoral/ano_eleicao=2024/raw/
```

#### Silver

- abre cada ZIP da Bronze e trata separadamente os quatro CSVs `fefc_genero`,
  `fefc_cor_raca`, `fp_genero` e `fp_cor_raca`
- aplica contratos explicitos para 2020 (municipal), 2022 (geral) e 2024 (municipal)
- padroniza nomes de colunas e valores categoricos
- converte inteiros, valores monetarios, percentuais, datas e horas
- converte sentinelas vazias e valores numericos invalidos, como `#########`, em nulo
- preserva arquivo e numero da linha de origem para rastreabilidade
- registra contagens e conversoes invalidas no `_manifest.json` de cada ano
- grava a saida tratada em `silver/fundo_eleitoral/ano_eleicao=YYYY/tratado/`

Exemplo:

```text
s3://<bucket>/silver/fundo_eleitoral/ano_eleicao=2024/tratado/
```

#### Qualidade

- le os CSVs Silver diretamente do S3 apos o tratamento
- valida schema, campos obrigatorios, tipos, dominios e ano da particao
- identifica nulos, duplicidades no grao e divergencias de contagem
- verifica integridade geografica por esfera partidaria
- compara a cobertura entre os arquivos de genero e cor/raca
- registra percentuais fora de 0 a 100, ajustes negativos e nulos vindos da
  fonte como alertas nao bloqueantes
- interrompe a DAG quando encontra erros estruturais ou de integridade
- grava um relatorio por ano em
  `quality/fundo_eleitoral/ano_eleicao=YYYY/_quality_report.json`

#### Gold

- camada de consumo analitico
- PostgreSQL 16 local no banco `fefc_dw`
- modelo estrela com dimensoes compartilhadas e quatro tabelas fato
- fatos particionados por `ano_eleicao` em 2020, 2022 e 2024
- views analiticas prontas para consultas sem repetir todos os joins

### Modelo Analitico

As quatro tabelas fato preservam o grao de cada CSV:

| Tabela pai | Grao |
|---|---|
| `dw.fato_fefc_genero` | eleicao, partido e genero |
| `dw.fato_fefc_cor_raca` | eleicao, partido, genero e cor/raca |
| `dw.fato_fp_genero` | eleicao, partido, localidade e genero |
| `dw.fato_fp_cor_raca` | eleicao, partido, localidade, genero e cor/raca |

Cada fato possui particoes fisicas com sufixos `_2020`, `_2022` e `_2024`.
O particionamento e somente por ano; o tipo de CSV define o grao da tabela fato,
mas nao e uma chave adicional de particionamento.

Dimensoes:

- `dw.dim_eleicao`
- `dw.dim_partido`
- `dw.dim_genero`
- `dw.dim_cor_raca`
- `dw.dim_localidade`

Views para consumo:

- `dw.vw_fefc_genero`
- `dw.vw_fefc_cor_raca`
- `dw.vw_fp_genero`
- `dw.vw_fp_cor_raca`

A documentacao completa esta em
[`docs/modelo_analitico.md`](docs/modelo_analitico.md). O diagrama visual esta em
[`docs/diagrams/data_warehouse_erd.md`](docs/diagrams/data_warehouse_erd.md).

### Carga no Data Warehouse

A task `load_gold` carrega os CSVs Silver somente depois da validacao de
qualidade. A carga usa `COPY`, atualiza as dimensoes e substitui a fatia do mesmo
ano e arquivo para evitar duplicidades em reexecucoes.

Executar manualmente todos os anos validados:

```bash
docker compose run --rm warehouse-load
```

Executar anos selecionados:

```bash
docker compose run --rm warehouse-load python -m src.gold.analytical_loader --years 2022 2024
```

O resultado de cada arquivo fica registrado em `dw.carga_arquivo`.

### PostgreSQL Local

O servico `postgres` cria automaticamente o banco `fefc_dw`. Dentro do Docker,
ele e acessado por `postgres:5432`; no Windows, por `localhost:5438`.

```text
host: localhost
port: 5438
database: fefc_dw
user: fefc_user
password: fefc_password
schema: dw
```

### Migrations Manuais

Os arquivos SQL versionados ficam em `migrations/`. Para aplicar todas as
migrations pendentes:

```bash
docker compose run --rm warehouse-migrations
```

Para consultar o status:

```bash
docker compose run --rm warehouse-migrations python -m src.gold.migrations status
```

Tambem e possivel executar pelo container do Airflow:

```bash
docker compose exec airflow-webserver python -m src.gold.migrations up
docker compose exec airflow-webserver python -m src.gold.migrations status
```

O executor registra versao e checksum em `public.dw_schema_migrations`, rejeita
arquivos alterados depois da aplicacao e usa lock transacional para impedir duas
execucoes simultaneas.

Para listar as particoes criadas:

```bash
docker compose exec postgres psql -U fefc_user -d fefc_dw -c "SELECT relid::regclass, level FROM pg_partition_tree('dw.fato_fp_genero');"
```

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

- URL: http://localhost:8686
- usuario padrao: `airflow`
- senha padrao: `airflow`

## Como Acessar o PostgreSQL

O Compose sobe um PostgreSQL local com dois propositos:

- database `fefc_dw` para o MVP do Data Warehouse
- database `airflow` para o metadata database do Airflow

Conexao padrao:

- host: `localhost`
- porta: `5438`
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

## Como Validar os Dados no LocalStack

Depois de rodar a DAG, voce pode confirmar que os arquivos caíram no S3 local com
o comando abaixo:

```bash
aws s3 ls s3://fefc-data-lake --recursive --endpoint-url http://localhost:4566
```

Outras formas praticas de validar:

```bash
aws s3 ls s3://fefc-data-lake/bronze/fundo_eleitoral/ano_eleicao=2024/raw/ --endpoint-url http://localhost:4566
```

```bash
aws s3 cp s3://fefc-data-lake/bronze/fundo_eleitoral/ano_eleicao=2024/raw/fefc_fp_2024.zip ./fefc_fp_2024.zip --endpoint-url http://localhost:4566
```

```bash
aws s3api head-object --bucket fefc-data-lake --key bronze/fundo_eleitoral/ano_eleicao=2024/raw/fefc_fp_2024.zip --endpoint-url http://localhost:4566
```

```bash
aws s3 ls s3://fefc-data-lake/silver/fundo_eleitoral/ano_eleicao=2024/tratado/ --endpoint-url http://localhost:4566
```

Liste os relatorios de qualidade particionados por ano:

```bash
aws s3 ls s3://fefc-data-lake/quality/fundo_eleitoral/ --recursive --endpoint-url http://localhost:4566
```

Abra o relatorio de uma eleicao diretamente no terminal:

```bash
aws s3 cp s3://fefc-data-lake/quality/fundo_eleitoral/ano_eleicao=2024/_quality_report.json - --endpoint-url http://localhost:4566
```

Se preferir, também vale conferir os logs da task `ingest` no Airflow, que mostram
os anos processados e o total de arquivos enviados para a Bronze.

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
- DAG do Airflow agendada diariamente as 12:00
- configuracao por variaveis de ambiente
- DAG inicial do Airflow
- abstracao de S3
- tratamento e padronizacao da camada Silver por ano de eleicao
- qualidade de dados com relatorios particionados no S3
- camada inicial de acesso ao PostgreSQL
- testes basicos
- Docker Compose local
- documentacao inicial

## O que Ficou Como Placeholder

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
4. conectar os proximos passos da Sprint 1 ao arquivo bruto ja ingerido
