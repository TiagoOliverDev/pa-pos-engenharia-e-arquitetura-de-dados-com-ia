# Modelo Analitico FEFC

O modelo usa quatro tabelas fato para preservar o grao de cada CSV. Cada fato e
uma tabela logica particionada por `ano_eleicao`, com particoes fisicas para
2020, 2022 e 2024.

```mermaid
erDiagram
    DIM_ELEICAO ||--o{ DIM_PARTIDO : possui
    DIM_ELEICAO ||--o{ DIM_LOCALIDADE : contextualiza
    DIM_ELEICAO ||--o{ FATO_FEFC_GENERO : particiona
    DIM_ELEICAO ||--o{ FATO_FEFC_COR_RACA : particiona
    DIM_ELEICAO ||--o{ FATO_FP_GENERO : particiona
    DIM_ELEICAO ||--o{ FATO_FP_COR_RACA : particiona

    DIM_PARTIDO ||--o{ FATO_FEFC_GENERO : classifica
    DIM_PARTIDO ||--o{ FATO_FEFC_COR_RACA : classifica
    DIM_PARTIDO ||--o{ FATO_FP_GENERO : classifica
    DIM_PARTIDO ||--o{ FATO_FP_COR_RACA : classifica

    DIM_GENERO ||--o{ FATO_FEFC_GENERO : segmenta
    DIM_GENERO ||--o{ FATO_FEFC_COR_RACA : segmenta
    DIM_GENERO ||--o{ FATO_FP_GENERO : segmenta
    DIM_GENERO ||--o{ FATO_FP_COR_RACA : segmenta

    DIM_COR_RACA ||--o{ FATO_FEFC_COR_RACA : segmenta
    DIM_COR_RACA ||--o{ FATO_FP_COR_RACA : segmenta

    DIM_LOCALIDADE ||--o{ FATO_FP_GENERO : localiza
    DIM_LOCALIDADE ||--o{ FATO_FP_COR_RACA : localiza

    DIM_ELEICAO {
        smallint ano_eleicao PK
        varchar tipo_eleicao
        varchar descricao
    }
    DIM_PARTIDO {
        bigint partido_id PK
        smallint ano_eleicao FK
        int numero_partido
        varchar sigla_partido
    }
    DIM_GENERO {
        smallint genero_id PK
        varchar genero
    }
    DIM_COR_RACA {
        smallint cor_raca_id PK
        varchar cor_raca
    }
    DIM_LOCALIDADE {
        bigint localidade_id PK
        smallint ano_eleicao FK
        varchar esfera_partidaria
        char sigla_uf
        varchar sigla_ue
        varchar municipio
    }
    FATO_FEFC_GENERO {
        smallint ano_eleicao PK,FK
        bigint fato_id PK
        bigint partido_id FK
        smallint genero_id FK
        int quantidade_candidatos
        numeric valor_total_recebido_fefc
    }
    FATO_FEFC_COR_RACA {
        smallint ano_eleicao PK,FK
        bigint fato_id PK
        bigint partido_id FK
        smallint genero_id FK
        smallint cor_raca_id FK
        int quantidade_candidatos
        numeric valor_total_recebido_fefc
    }
    FATO_FP_GENERO {
        smallint ano_eleicao PK,FK
        bigint fato_id PK
        bigint partido_id FK
        bigint localidade_id FK
        smallint genero_id FK
        int quantidade_candidatos
        numeric valor_total_recebido_fp
    }
    FATO_FP_COR_RACA {
        smallint ano_eleicao PK,FK
        bigint fato_id PK
        bigint partido_id FK
        bigint localidade_id FK
        smallint genero_id FK
        smallint cor_raca_id FK
        int quantidade_candidatos
        numeric valor_total_recebido_fp
    }
```

## Particionamento

Cada tabela fato possui tres particoes `LIST` por ano. Por exemplo:

```text
dw.fato_fp_genero
|-- dw.fato_fp_genero_2020
|-- dw.fato_fp_genero_2022
`-- dw.fato_fp_genero_2024
```

Esse desenho permite consultar todos os anos pela tabela pai e ainda obter
`partition pruning` quando a consulta filtra `ano_eleicao`.
