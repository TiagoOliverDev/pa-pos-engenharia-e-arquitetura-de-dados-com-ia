CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_eleicao (
    ano_eleicao SMALLINT PRIMARY KEY,
    tipo_eleicao VARCHAR(20) NOT NULL,
    descricao VARCHAR(100) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_dim_eleicao_tipo
        CHECK (tipo_eleicao IN ('municipal', 'geral'))
);

INSERT INTO dw.dim_eleicao (ano_eleicao, tipo_eleicao, descricao)
VALUES
    (2020, 'municipal', 'Eleicoes Municipais 2020'),
    (2022, 'geral', 'Eleicoes Gerais 2022'),
    (2024, 'municipal', 'Eleicoes Municipais 2024')
ON CONFLICT (ano_eleicao) DO UPDATE
SET tipo_eleicao = EXCLUDED.tipo_eleicao,
    descricao = EXCLUDED.descricao;

CREATE TABLE dw.dim_partido (
    partido_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ano_eleicao SMALLINT NOT NULL,
    numero_partido INTEGER NOT NULL,
    sigla_partido VARCHAR(30) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_dim_partido_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT uq_dim_partido_ano_numero
        UNIQUE (ano_eleicao, numero_partido),
    CONSTRAINT uq_dim_partido_id_ano
        UNIQUE (partido_id, ano_eleicao),
    CONSTRAINT ck_dim_partido_numero
        CHECK (numero_partido > 0)
);

CREATE TABLE dw.dim_genero (
    genero_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    genero VARCHAR(30) NOT NULL UNIQUE
);

INSERT INTO dw.dim_genero (genero)
VALUES ('FEMININO'), ('MASCULINO')
ON CONFLICT (genero) DO NOTHING;

CREATE TABLE dw.dim_cor_raca (
    cor_raca_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cor_raca VARCHAR(30) NOT NULL UNIQUE
);

INSERT INTO dw.dim_cor_raca (cor_raca)
VALUES ('NEGRA'), ('NÃO NEGRA')
ON CONFLICT (cor_raca) DO NOTHING;

CREATE TABLE dw.dim_localidade (
    localidade_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ano_eleicao SMALLINT NOT NULL,
    esfera_partidaria VARCHAR(20) NOT NULL,
    sigla_uf CHAR(2),
    sigla_ue VARCHAR(10),
    municipio VARCHAR(150),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_dim_localidade_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT uq_dim_localidade_natural
        UNIQUE NULLS NOT DISTINCT (
            ano_eleicao,
            esfera_partidaria,
            sigla_uf,
            sigla_ue,
            municipio
        ),
    CONSTRAINT uq_dim_localidade_id_ano
        UNIQUE (localidade_id, ano_eleicao),
    CONSTRAINT ck_dim_localidade_esfera
        CHECK (esfera_partidaria IN ('NACIONAL', 'ESTADUAL', 'MUNICIPAL')),
    CONSTRAINT ck_dim_localidade_geografia
        CHECK (
            (esfera_partidaria = 'NACIONAL'
                AND sigla_uf IS NULL
                AND sigla_ue IS NULL
                AND municipio IS NULL)
            OR
            (esfera_partidaria = 'ESTADUAL'
                AND sigla_uf IS NOT NULL
                AND sigla_ue IS NULL
                AND municipio IS NULL)
            OR
            (esfera_partidaria = 'MUNICIPAL'
                AND sigla_uf IS NOT NULL
                AND sigla_ue IS NOT NULL
                AND municipio IS NOT NULL)
        )
);

CREATE TABLE dw.fato_fefc_genero (
    fato_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ano_eleicao SMALLINT NOT NULL,
    partido_id BIGINT NOT NULL,
    genero_id SMALLINT NOT NULL,
    quantidade_candidatos INTEGER NOT NULL,
    valor_partido_fefc NUMERIC(20, 2) NOT NULL,
    percentual_candidatos_partido_genero NUMERIC(12, 6) NOT NULL,
    valor_repasse_minimo_cota NUMERIC(20, 2) NOT NULL,
    valor_total_recebido_fefc NUMERIC(20, 2) NOT NULL,
    percentual_valor_fefc_genero NUMERIC(12, 6) NOT NULL,
    status_renuncia SMALLINT,
    data_hora_geracao TIMESTAMP NOT NULL,
    source_archive TEXT NOT NULL,
    source_member TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ano_eleicao, fato_id),
    CONSTRAINT fk_fefc_genero_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT fk_fefc_genero_partido
        FOREIGN KEY (partido_id, ano_eleicao)
        REFERENCES dw.dim_partido (partido_id, ano_eleicao),
    CONSTRAINT fk_fefc_genero_genero
        FOREIGN KEY (genero_id) REFERENCES dw.dim_genero (genero_id),
    CONSTRAINT uq_fefc_genero_source
        UNIQUE (ano_eleicao, source_member, source_row_number),
    CONSTRAINT ck_fefc_genero_quantidade CHECK (quantidade_candidatos >= 0),
    CONSTRAINT ck_fefc_genero_status CHECK (status_renuncia IN (0, 1))
) PARTITION BY LIST (ano_eleicao);

CREATE TABLE dw.fato_fefc_cor_raca (
    fato_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ano_eleicao SMALLINT NOT NULL,
    partido_id BIGINT NOT NULL,
    genero_id SMALLINT NOT NULL,
    cor_raca_id SMALLINT NOT NULL,
    quantidade_candidatos INTEGER NOT NULL,
    valor_partido_fefc NUMERIC(20, 2) NOT NULL,
    percentual_candidatos_partido_genero NUMERIC(12, 6) NOT NULL,
    valor_repasse_minimo_cota NUMERIC(20, 2) NOT NULL,
    valor_total_recebido_fefc NUMERIC(20, 2) NOT NULL,
    percentual_valor_fefc_genero NUMERIC(12, 6) NOT NULL,
    status_renuncia SMALLINT,
    data_hora_geracao TIMESTAMP NOT NULL,
    source_archive TEXT NOT NULL,
    source_member TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ano_eleicao, fato_id),
    CONSTRAINT fk_fefc_cor_raca_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT fk_fefc_cor_raca_partido
        FOREIGN KEY (partido_id, ano_eleicao)
        REFERENCES dw.dim_partido (partido_id, ano_eleicao),
    CONSTRAINT fk_fefc_cor_raca_genero
        FOREIGN KEY (genero_id) REFERENCES dw.dim_genero (genero_id),
    CONSTRAINT fk_fefc_cor_raca_cor_raca
        FOREIGN KEY (cor_raca_id) REFERENCES dw.dim_cor_raca (cor_raca_id),
    CONSTRAINT uq_fefc_cor_raca_source
        UNIQUE (ano_eleicao, source_member, source_row_number),
    CONSTRAINT ck_fefc_cor_raca_quantidade CHECK (quantidade_candidatos >= 0),
    CONSTRAINT ck_fefc_cor_raca_status CHECK (status_renuncia IN (0, 1))
) PARTITION BY LIST (ano_eleicao);

CREATE TABLE dw.fato_fp_genero (
    fato_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ano_eleicao SMALLINT NOT NULL,
    partido_id BIGINT NOT NULL,
    localidade_id BIGINT NOT NULL,
    genero_id SMALLINT NOT NULL,
    quantidade_candidatos INTEGER NOT NULL,
    valor_despesa_diretorio_fp NUMERIC(20, 2) NOT NULL,
    percentual_candidatos_partido_genero NUMERIC(12, 6) NOT NULL,
    valor_despesa_minimo_cota NUMERIC(20, 2) NOT NULL,
    valor_total_recebido_fp NUMERIC(20, 2) NOT NULL,
    percentual_valor_fp_genero NUMERIC(12, 6),
    data_hora_geracao TIMESTAMP NOT NULL,
    source_archive TEXT NOT NULL,
    source_member TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ano_eleicao, fato_id),
    CONSTRAINT fk_fp_genero_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT fk_fp_genero_partido
        FOREIGN KEY (partido_id, ano_eleicao)
        REFERENCES dw.dim_partido (partido_id, ano_eleicao),
    CONSTRAINT fk_fp_genero_localidade
        FOREIGN KEY (localidade_id, ano_eleicao)
        REFERENCES dw.dim_localidade (localidade_id, ano_eleicao),
    CONSTRAINT fk_fp_genero_genero
        FOREIGN KEY (genero_id) REFERENCES dw.dim_genero (genero_id),
    CONSTRAINT uq_fp_genero_source
        UNIQUE (ano_eleicao, source_member, source_row_number),
    CONSTRAINT ck_fp_genero_quantidade CHECK (quantidade_candidatos >= 0)
) PARTITION BY LIST (ano_eleicao);

CREATE TABLE dw.fato_fp_cor_raca (
    fato_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ano_eleicao SMALLINT NOT NULL,
    partido_id BIGINT NOT NULL,
    localidade_id BIGINT NOT NULL,
    genero_id SMALLINT NOT NULL,
    cor_raca_id SMALLINT NOT NULL,
    quantidade_candidatos INTEGER NOT NULL,
    valor_despesa_diretorio_fp NUMERIC(20, 2) NOT NULL,
    percentual_candidatos_partido_genero NUMERIC(12, 6) NOT NULL,
    valor_despesa_minimo_cota NUMERIC(20, 2) NOT NULL,
    valor_total_recebido_fp NUMERIC(20, 2) NOT NULL,
    percentual_valor_fp_genero NUMERIC(12, 6),
    data_hora_geracao TIMESTAMP NOT NULL,
    source_archive TEXT NOT NULL,
    source_member TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ano_eleicao, fato_id),
    CONSTRAINT fk_fp_cor_raca_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT fk_fp_cor_raca_partido
        FOREIGN KEY (partido_id, ano_eleicao)
        REFERENCES dw.dim_partido (partido_id, ano_eleicao),
    CONSTRAINT fk_fp_cor_raca_localidade
        FOREIGN KEY (localidade_id, ano_eleicao)
        REFERENCES dw.dim_localidade (localidade_id, ano_eleicao),
    CONSTRAINT fk_fp_cor_raca_genero
        FOREIGN KEY (genero_id) REFERENCES dw.dim_genero (genero_id),
    CONSTRAINT fk_fp_cor_raca_cor_raca
        FOREIGN KEY (cor_raca_id) REFERENCES dw.dim_cor_raca (cor_raca_id),
    CONSTRAINT uq_fp_cor_raca_source
        UNIQUE (ano_eleicao, source_member, source_row_number),
    CONSTRAINT ck_fp_cor_raca_quantidade CHECK (quantidade_candidatos >= 0)
) PARTITION BY LIST (ano_eleicao);

CREATE TABLE dw.fato_fefc_genero_2020
    PARTITION OF dw.fato_fefc_genero FOR VALUES IN (2020);
CREATE TABLE dw.fato_fefc_genero_2022
    PARTITION OF dw.fato_fefc_genero FOR VALUES IN (2022);
CREATE TABLE dw.fato_fefc_genero_2024
    PARTITION OF dw.fato_fefc_genero FOR VALUES IN (2024);

CREATE TABLE dw.fato_fefc_cor_raca_2020
    PARTITION OF dw.fato_fefc_cor_raca FOR VALUES IN (2020);
CREATE TABLE dw.fato_fefc_cor_raca_2022
    PARTITION OF dw.fato_fefc_cor_raca FOR VALUES IN (2022);
CREATE TABLE dw.fato_fefc_cor_raca_2024
    PARTITION OF dw.fato_fefc_cor_raca FOR VALUES IN (2024);

CREATE TABLE dw.fato_fp_genero_2020
    PARTITION OF dw.fato_fp_genero FOR VALUES IN (2020);
CREATE TABLE dw.fato_fp_genero_2022
    PARTITION OF dw.fato_fp_genero FOR VALUES IN (2022);
CREATE TABLE dw.fato_fp_genero_2024
    PARTITION OF dw.fato_fp_genero FOR VALUES IN (2024);

CREATE TABLE dw.fato_fp_cor_raca_2020
    PARTITION OF dw.fato_fp_cor_raca FOR VALUES IN (2020);
CREATE TABLE dw.fato_fp_cor_raca_2022
    PARTITION OF dw.fato_fp_cor_raca FOR VALUES IN (2022);
CREATE TABLE dw.fato_fp_cor_raca_2024
    PARTITION OF dw.fato_fp_cor_raca FOR VALUES IN (2024);

CREATE INDEX ix_fefc_genero_partido_genero
    ON dw.fato_fefc_genero (ano_eleicao, partido_id, genero_id);
CREATE INDEX ix_fefc_cor_raca_dimensoes
    ON dw.fato_fefc_cor_raca (
        ano_eleicao,
        partido_id,
        genero_id,
        cor_raca_id
    );
CREATE INDEX ix_fp_genero_dimensoes
    ON dw.fato_fp_genero (
        ano_eleicao,
        partido_id,
        localidade_id,
        genero_id
    );
CREATE INDEX ix_fp_cor_raca_dimensoes
    ON dw.fato_fp_cor_raca (
        ano_eleicao,
        partido_id,
        localidade_id,
        genero_id,
        cor_raca_id
    );

CREATE VIEW dw.vw_fefc_genero AS
SELECT
    f.ano_eleicao,
    e.tipo_eleicao,
    p.numero_partido,
    p.sigla_partido,
    g.genero,
    f.quantidade_candidatos,
    f.valor_partido_fefc,
    f.percentual_candidatos_partido_genero,
    f.valor_repasse_minimo_cota,
    f.valor_total_recebido_fefc,
    f.percentual_valor_fefc_genero,
    f.status_renuncia,
    f.data_hora_geracao
FROM dw.fato_fefc_genero f
JOIN dw.dim_eleicao e USING (ano_eleicao)
JOIN dw.dim_partido p
  ON p.partido_id = f.partido_id AND p.ano_eleicao = f.ano_eleicao
JOIN dw.dim_genero g USING (genero_id);

CREATE VIEW dw.vw_fefc_cor_raca AS
SELECT
    f.ano_eleicao,
    e.tipo_eleicao,
    p.numero_partido,
    p.sigla_partido,
    g.genero,
    c.cor_raca,
    f.quantidade_candidatos,
    f.valor_partido_fefc,
    f.percentual_candidatos_partido_genero,
    f.valor_repasse_minimo_cota,
    f.valor_total_recebido_fefc,
    f.percentual_valor_fefc_genero,
    f.status_renuncia,
    f.data_hora_geracao
FROM dw.fato_fefc_cor_raca f
JOIN dw.dim_eleicao e USING (ano_eleicao)
JOIN dw.dim_partido p
  ON p.partido_id = f.partido_id AND p.ano_eleicao = f.ano_eleicao
JOIN dw.dim_genero g USING (genero_id)
JOIN dw.dim_cor_raca c USING (cor_raca_id);

CREATE VIEW dw.vw_fp_genero AS
SELECT
    f.ano_eleicao,
    e.tipo_eleicao,
    p.numero_partido,
    p.sigla_partido,
    l.esfera_partidaria,
    l.sigla_uf,
    l.sigla_ue,
    l.municipio,
    g.genero,
    f.quantidade_candidatos,
    f.valor_despesa_diretorio_fp,
    f.percentual_candidatos_partido_genero,
    f.valor_despesa_minimo_cota,
    f.valor_total_recebido_fp,
    f.percentual_valor_fp_genero,
    f.data_hora_geracao
FROM dw.fato_fp_genero f
JOIN dw.dim_eleicao e USING (ano_eleicao)
JOIN dw.dim_partido p
  ON p.partido_id = f.partido_id AND p.ano_eleicao = f.ano_eleicao
JOIN dw.dim_localidade l
  ON l.localidade_id = f.localidade_id AND l.ano_eleicao = f.ano_eleicao
JOIN dw.dim_genero g USING (genero_id);

CREATE VIEW dw.vw_fp_cor_raca AS
SELECT
    f.ano_eleicao,
    e.tipo_eleicao,
    p.numero_partido,
    p.sigla_partido,
    l.esfera_partidaria,
    l.sigla_uf,
    l.sigla_ue,
    l.municipio,
    g.genero,
    c.cor_raca,
    f.quantidade_candidatos,
    f.valor_despesa_diretorio_fp,
    f.percentual_candidatos_partido_genero,
    f.valor_despesa_minimo_cota,
    f.valor_total_recebido_fp,
    f.percentual_valor_fp_genero,
    f.data_hora_geracao
FROM dw.fato_fp_cor_raca f
JOIN dw.dim_eleicao e USING (ano_eleicao)
JOIN dw.dim_partido p
  ON p.partido_id = f.partido_id AND p.ano_eleicao = f.ano_eleicao
JOIN dw.dim_localidade l
  ON l.localidade_id = f.localidade_id AND l.ano_eleicao = f.ano_eleicao
JOIN dw.dim_genero g USING (genero_id)
JOIN dw.dim_cor_raca c USING (cor_raca_id);
