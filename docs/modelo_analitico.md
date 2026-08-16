# Modelo Analitico do Data Warehouse FEFC

## Objetivo

O Data Warehouse do projeto FEFC organiza os dados tratados das eleicoes de
2020, 2022 e 2024 para consultas por partido, genero, cor/raca, esfera
partidaria e localidade.

O modelo foi implementado no PostgreSQL 16, no banco `fefc_dw` e no schema
`dw`. O DDL versionado esta em
[`migrations/001_create_analytical_model.sql`](../migrations/001_create_analytical_model.sql).

## Decisao de Modelagem

Foi adotado um modelo estrela com dimensoes compartilhadas e quatro tabelas
fato. Os quatro CSVs da fonte possuem graos e medidas diferentes; por isso, eles
nao foram combinados em uma unica tabela.

O particionamento foi definido somente por `ano_eleicao`.

Essa abordagem foi escolhida porque:

- o periodo eleitoral e o filtro mais comum do MVP;
- cada nova eleicao pode ser adicionada como uma nova particao;
- o PostgreSQL elimina particoes desnecessarias com `partition pruning`;
- uma tabela pai permite consultar todos os anos sem `UNION` manual;
- o tipo de CSV continua representado por uma tabela fato com grao proprio;
- evita misturar registros agregados por genero com registros por cor/raca.

Fisicamente existem tres particoes por fato, mas analistas consultam apenas as
tabelas pai.

## Fluxo dos Dados

```text
TSE
  -> S3 Bronze: ZIP original
  -> S3 Silver: CSV tratado e particionado por ano
  -> Qualidade: relatorio por ano
  -> PostgreSQL Gold: dimensoes, fatos e views
```

## Grao das Tabelas Fato

| Tabela | Fonte Silver | Grao de uma linha |
|---|---|---|
| `dw.fato_fefc_genero` | `fefc_genero` | eleicao + partido + genero |
| `dw.fato_fefc_cor_raca` | `fefc_cor_raca` | eleicao + partido + genero + cor/raca |
| `dw.fato_fp_genero` | `fp_genero` | eleicao + partido + localidade + genero |
| `dw.fato_fp_cor_raca` | `fp_cor_raca` | eleicao + partido + localidade + genero + cor/raca |

Os campos `source_member` e `source_row_number`, combinados com
`ano_eleicao`, formam uma chave unica de rastreabilidade em cada fato. Essa
restricao prepara a carga para ser idempotente e evita inserir duas vezes a
mesma linha Silver.

## Dimensoes

### `dw.dim_eleicao`

Representa o contexto temporal e o tipo da eleicao.

| Coluna | Tipo | Regra |
|---|---|---|
| `ano_eleicao` | `SMALLINT` | chave primaria |
| `tipo_eleicao` | `VARCHAR(20)` | `municipal` ou `geral` |
| `descricao` | `VARCHAR(100)` | descricao para consumo analitico |
| `criado_em` | `TIMESTAMPTZ` | auditoria de criacao |

Valores iniciais: 2020 municipal, 2022 geral e 2024 municipal.

### `dw.dim_partido`

Mantem o partido no contexto de cada eleicao, preservando mudancas historicas
de numero ou sigla.

| Coluna | Tipo | Regra |
|---|---|---|
| `partido_id` | `BIGINT` | chave substituta |
| `ano_eleicao` | `SMALLINT` | FK para `dim_eleicao` |
| `numero_partido` | `INTEGER` | numero positivo |
| `sigla_partido` | `VARCHAR(30)` | sigla padronizada |
| `criado_em` | `TIMESTAMPTZ` | auditoria de criacao |

A chave natural e `ano_eleicao + numero_partido`.

### `dw.dim_genero`

Dimensao de baixa cardinalidade compartilhada pelos quatro fatos.

| Coluna | Tipo | Regra |
|---|---|---|
| `genero_id` | `SMALLINT` | chave substituta |
| `genero` | `VARCHAR(30)` | valor unico |

Valores iniciais: `FEMININO` e `MASCULINO`.

### `dw.dim_cor_raca`

Utilizada apenas pelos fatos com detalhamento de cor/raca.

| Coluna | Tipo | Regra |
|---|---|---|
| `cor_raca_id` | `SMALLINT` | chave substituta |
| `cor_raca` | `VARCHAR(30)` | valor unico |

Valores iniciais: `NEGRA` e `NÃO NEGRA`.

### `dw.dim_localidade`

Representa a esfera partidaria e o nivel geografico dos arquivos de Fundo
Partidario (`fp_*`).

| Coluna | Tipo | Regra |
|---|---|---|
| `localidade_id` | `BIGINT` | chave substituta |
| `ano_eleicao` | `SMALLINT` | FK para `dim_eleicao` |
| `esfera_partidaria` | `VARCHAR(20)` | nacional, estadual ou municipal |
| `sigla_uf` | `CHAR(2)` | obrigatoria a partir da esfera estadual |
| `sigla_ue` | `VARCHAR(10)` | obrigatoria na esfera municipal |
| `municipio` | `VARCHAR(150)` | obrigatorio na esfera municipal |
| `chave_natural` | `TEXT` | chave gerada e indexada para carga eficiente |
| `criado_em` | `TIMESTAMPTZ` | auditoria de criacao |

Regras de integridade:

- nacional: UF, UE e municipio nulos;
- estadual: UF preenchida, UE e municipio nulos;
- municipal: UF, UE e municipio preenchidos.

## Fatos FEFC

As tabelas FEFC armazenam valores do Fundo Especial de Financiamento de
Campanha.

### Medidas compartilhadas

| Coluna | Significado |
|---|---|
| `quantidade_candidatos` | quantidade de candidatos no segmento |
| `valor_partido_fefc` | valor FEFC atribuido ao partido |
| `percentual_candidatos_partido_genero` | participacao de candidatos no partido |
| `valor_repasse_minimo_cota` | valor minimo calculado para a cota |
| `valor_total_recebido_fefc` | valor FEFC efetivamente recebido |
| `percentual_valor_fefc_genero` | percentual do valor FEFC no segmento |
| `status_renuncia` | indicador de renuncia, quando informado |

`dw.fato_fefc_cor_raca` acrescenta a FK `cor_raca_id`. Os dois fatos possuem
FKs para eleicao, partido e genero.

## Fatos FP

As tabelas FP armazenam distribuicoes e recebimentos relacionados ao Fundo
Partidario.

### Medidas compartilhadas

| Coluna | Significado |
|---|---|
| `quantidade_candidatos` | quantidade de candidatos no segmento |
| `valor_despesa_diretorio_fp` | despesa do diretorio com Fundo Partidario |
| `percentual_candidatos_partido_genero` | participacao de candidatos no partido |
| `valor_despesa_minimo_cota` | valor minimo calculado para a cota |
| `valor_total_recebido_fp` | valor de Fundo Partidario recebido |
| `percentual_valor_fp_genero` | percentual do valor no segmento; pode ser nulo |

`dw.fato_fp_cor_raca` acrescenta a FK `cor_raca_id`. Os dois fatos possuem FKs
para eleicao, partido, localidade e genero.

Valores financeiros utilizam `NUMERIC`, evitando perda de centavos. Os
percentuais utilizam escala maior para preservar valores atipicos identificados
na fonte, inclusive percentuais superiores a 100 ou negativos.

## Rastreabilidade e Auditoria

Todas as tabelas fato possuem:

- `data_hora_geracao`: momento informado pelo TSE;
- `source_archive`: chave do ZIP Bronze;
- `source_member`: CSV de origem dentro do ZIP;
- `source_row_number`: linha original do CSV;
- `carregado_em`: momento de insercao no Data Warehouse.

Esses campos permitem retornar de uma linha analitica ate o objeto e a linha de
origem.

## Particionamento Fisico

As quatro tabelas fato usam `PARTITION BY LIST (ano_eleicao)`.

Exemplo:

```text
dw.fato_fp_genero
|-- dw.fato_fp_genero_2020
|-- dw.fato_fp_genero_2022
`-- dw.fato_fp_genero_2024
```

Uma consulta deve usar a tabela pai:

```sql
SELECT
    ano_eleicao,
    SUM(valor_total_recebido_fp) AS valor_recebido
FROM dw.fato_fp_genero
WHERE ano_eleicao = 2024
GROUP BY ano_eleicao;
```

O filtro permite que o PostgreSQL consulte somente a particao de 2024.

Para conferir as particoes:

```sql
SELECT relid::regclass AS tabela, level
FROM pg_partition_tree('dw.fato_fp_genero');
```

## Views Analiticas

As views combinam fatos e dimensoes, disponibilizando nomes e atributos em vez
de apenas chaves substitutas.

| View | Uso principal |
|---|---|
| `dw.vw_fefc_genero` | analise FEFC por partido e genero |
| `dw.vw_fefc_cor_raca` | analise FEFC por genero e cor/raca |
| `dw.vw_fp_genero` | analise FP por localidade e genero |
| `dw.vw_fp_cor_raca` | analise FP por localidade, genero e cor/raca |

Exemplo de consulta:

```sql
SELECT
    ano_eleicao,
    sigla_partido,
    genero,
    SUM(valor_total_recebido_fefc) AS total_recebido
FROM dw.vw_fefc_genero
GROUP BY ano_eleicao, sigla_partido, genero
ORDER BY ano_eleicao, sigla_partido, genero;
```

## Relacionamentos

O diagrama entidade-relacionamento esta em
[`docs/diagrams/data_warehouse_erd.md`](diagrams/data_warehouse_erd.md).

Resumo das cardinalidades:

- uma eleicao possui muitos partidos e localidades;
- uma eleicao possui muitos registros em cada fato;
- um partido participa de muitos registros fato no mesmo ano;
- um genero participa de muitos registros fato;
- uma cor/raca participa dos fatos com esse detalhamento;
- uma localidade participa dos fatos de Fundo Partidario.

## Migrations

O historico de migrations e controlado por `public.dw_schema_migrations`.

Aplicar migrations pendentes:

```bash
docker compose run --rm warehouse-migrations
```

Consultar status:

```bash
docker compose run --rm warehouse-migrations python -m src.gold.migrations status
```

O executor valida checksum, aplica as migrations em transacao e utiliza lock
para impedir execucoes concorrentes.

## Carga da Silver

A carga e implementada em
[`src/gold/analytical_loader.py`](../src/gold/analytical_loader.py) e executada
pela task `load_gold` da DAG somente depois de um relatorio de qualidade valido.

Fluxo de cada artefato:

1. baixa o CSV tratado do S3;
2. confere schema e contagem contra o manifesto Silver;
3. usa `COPY` para uma tabela temporaria PostgreSQL;
4. insere ou atualiza partido, genero, cor/raca e localidade;
5. remove somente a fatia anterior do mesmo ano e arquivo;
6. insere na tabela fato pai, deixando o PostgreSQL selecionar a particao;
7. registra o resultado em `dw.carga_arquivo`.

Os 12 artefatos sao carregados em uma unica transacao. Se qualquer arquivo
falhar, toda a execucao e revertida. A substituicao por `ano_eleicao +
source_member` torna a carga idempotente e tambem remove registros que tenham
desaparecido em uma revisao posterior do TSE.

A dimensao de localidade possui `chave_natural` gerada e indexada para tornar o
join dos arquivos municipais eficiente sem perder a semantica de campos nulos.

Executar a carga manual dos anos do MVP:

```bash
docker compose run --rm warehouse-load
```

Executar somente anos selecionados:

```bash
docker compose run --rm warehouse-load python -m src.gold.analytical_loader --years 2022 2024
```

A carga manual exige que `_quality_report.json` esteja presente e marcado como
valido para cada ano solicitado.

Consultar a auditoria:

```sql
SELECT
    ano_eleicao,
    dataset_name,
    linhas_origem,
    linhas_removidas,
    linhas_inseridas,
    carregado_em
FROM dw.carga_arquivo
ORDER BY ano_eleicao, dataset_name;
```

Validar as contagens por particao:

```sql
SELECT tableoid::regclass AS particao, COUNT(*) AS linhas
FROM dw.fato_fp_genero
GROUP BY tableoid
ORDER BY particao;
```

## Conexao Local

| Configuracao | Dentro do Docker | No host Windows |
|---|---|---|
| Host | `postgres` | `localhost` |
| Porta | `5432` | `5438` |
| Banco | `fefc_dw` | `fefc_dw` |
| Usuario | `fefc_user` | `fefc_user` |
| Schema | `dw` | `dw` |

## Evolucao do Modelo

Para incluir uma nova eleicao:

1. adicionar o ano em `dw.dim_eleicao` por uma nova migration;
2. criar uma particao para o novo ano em cada uma das quatro tabelas fato;
3. manter a carga apontando para as tabelas pai;
4. executar os testes e verificar `partition pruning` no PostgreSQL.

Alteracoes em tabelas ou constraints devem ser feitas em uma nova migration. Um
arquivo de migration ja aplicado nunca deve ser editado, pois seu checksum fica
registrado no banco.
