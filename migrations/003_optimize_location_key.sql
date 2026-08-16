ALTER TABLE dw.dim_localidade
ADD COLUMN chave_natural TEXT GENERATED ALWAYS AS (
    ano_eleicao::TEXT
    || '|'
    || esfera_partidaria
    || '|'
    || COALESCE(BTRIM(sigla_uf), '')
    || '|'
    || COALESCE(sigla_ue, '')
    || '|'
    || COALESCE(municipio, '')
) STORED;

ALTER TABLE dw.dim_localidade
ADD CONSTRAINT uq_dim_localidade_chave_natural UNIQUE (chave_natural);
