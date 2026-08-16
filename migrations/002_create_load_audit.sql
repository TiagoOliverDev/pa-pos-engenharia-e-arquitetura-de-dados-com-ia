CREATE TABLE dw.carga_arquivo (
    carga_arquivo_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ano_eleicao SMALLINT NOT NULL,
    dataset_name VARCHAR(40) NOT NULL,
    source_member TEXT NOT NULL,
    output_key TEXT NOT NULL,
    linhas_origem INTEGER NOT NULL,
    linhas_removidas INTEGER NOT NULL,
    linhas_inseridas INTEGER NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_carga_arquivo_eleicao
        FOREIGN KEY (ano_eleicao) REFERENCES dw.dim_eleicao (ano_eleicao),
    CONSTRAINT uq_carga_arquivo_dataset
        UNIQUE (ano_eleicao, dataset_name),
    CONSTRAINT ck_carga_arquivo_dataset
        CHECK (
            dataset_name IN (
                'fefc_genero',
                'fefc_cor_raca',
                'fp_genero',
                'fp_cor_raca'
            )
        ),
    CONSTRAINT ck_carga_arquivo_linhas
        CHECK (
            linhas_origem > 0
            AND linhas_removidas >= 0
            AND linhas_inseridas > 0
        )
);
