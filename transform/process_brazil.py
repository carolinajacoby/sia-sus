import polars as pl
from pathlib import Path
import time

# Caminhos
PROJETO_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJETO_ROOT / "data/processed/am_2024_consolidado_final.zstd.parquet"
PROCESSED_FILE = PROJETO_ROOT / "data/processed/am_2024_tratado.zstd.parquet"

# Mapeamento de nomes de colunas
RENAME_MAP = {
    "AP_MVM": "data_movimento",
    "AP_CONDIC": "tipo_gestao",
    "AP_GESTAO": "codigo_uf_municipio_gestao",
    "AP_CODUNI": "codigo_estabelecimento",
    "AP_AUTORIZ": "numero_apac",
    "AP_CMP": "data_atendimento",
    "AP_PRIPAL": "procedimento_principal",
    "AP_VL_AP": "valor_apac",
    "AP_UFMUN": "uf_municipio_estabelecimento",
    "AP_TPUPS": "tipo_estabelecimento",
    "AP_TIPPRE": "tipo_prestador",
    "AP_MN_IND": "manutencao_individual",
    "AP_CNPJCPF": "cnpj_estabelecimento",
    "AP_CNPJMNT": "cnpj_mantenedora",
    "AP_CNSPCN": "cns_paciente",
    "AP_COIDADE": "codigo_idade",
    "AP_NUIDADE": "idade",
    "AP_SEXO": "sexo",
    "AP_RACACOR": "raca_cor",
    "AP_MUNPCN": "uf_municipio_residencia",
    "AP_UFNACIO": "codigo_nacionalidade",
    "AP_CEPPCN": "cep",
    "AP_UFDIF": "uf_diferente",
    "AP_MNDIF": "municipio_diferente",
    "AP_DTINIC": "data_inicio",
    "AP_DTFIM": "data_fim",
    "AP_TPATEN": "tipo_atendimento",
    "AP_TPAPAC": "indicador_apac",
    "AP_MOTSAI": "motivo_saida",
    "AP_OBITO": "obito",
    "AP_ENCERR": "encerramento",
    "AP_PERMAN": "permanencia",
    "AP_ALTA": "alta",
    "AP_TRANSF": "transferencia",
    "AP_DTOCOR": "data_ocorrencia",
    "AP_CODEMI": "codigo_orgao_emissor",
    "AP_CATEND": "carater_atendimento",
    "AP_APACANT": "numero_apac_anterior",
    "AP_UNISOL": "estabelecimento_solicitante",
    "AP_DTSOLIC": "data_solicitacao",
    "AP_DTAUT": "data_autorizacao",
    "AP_CIDCAS": "cid_causas_associadas",
    "AP_CIDPRI": "cid_principal",
    "AP_CIDSEC": "cid_secundario",
    "AP_ETNIA": "etnia",
    "AM_PESO": "peso",
    "AM_ALTURA": "altura",
    "AM_TRANSPL": "transplante",
    "AM_QTDTRAN": "quantidade_transplantes",
    "AM_GESTANT": "gestante"
}

# Tipos numéricos
DTYPE_MAP = {
    "valor_apac": pl.Float32,
    "idade": pl.Int32,
    "peso": pl.Float32,
    "altura": pl.Float32
}

# Colunas de data
DATE_COLS = [
    "data_movimento", "data_atendimento", "data_inicio", "data_fim",
    "data_ocorrencia", "data_solicitacao", "data_autorizacao"
]

# Função de parsing de datas compatível com Polars >=0.23
def parse_date_expression(col_name: str):
    return pl.when(pl.col(col_name).str.len_chars() == 8).then(
        pl.col(col_name).str.strptime(pl.Date, "%Y%m%d", strict=False)
    ).otherwise(
        pl.col(col_name).str.strptime(pl.Date, "%Y%m", strict=False)
    )


def main():
    print("🔹 Iniciando processamento nacional com Polars LazyFrame...")
    start = time.time()

    # Leitura preguiçosa do parquet
    df = pl.scan_parquet(RAW_FILE)

    # Renomeia colunas
    df = df.rename(RENAME_MAP)

    # Obter colunas do LazyFrame para evitar warnings
    schema_cols = df.collect_schema().names()

    # Conversões numéricas
    numeric_exprs = [
        pl.col(c).cast(dtype, strict=False).alias(c)
        for c, dtype in DTYPE_MAP.items()
        if c in schema_cols
    ]

    # Conversões de datas
    date_exprs = [parse_date_expression(c) for c in DATE_COLS if c in schema_cols]

    # Aplica todas as conversões de uma vez
    df = df.with_columns(numeric_exprs + date_exprs)

    # Salva de forma preguiçosa com compressão ZSTD
    print("Gravando arquivo tratado com compressão ZSTD...")
    df.sink_parquet(
        PROCESSED_FILE,
        compression="zstd",
        compression_level=7
    )

    elapsed = (time.time() - start) / 60
    size_gb = PROCESSED_FILE.stat().st_size / (1024 ** 3)

    print(f"✅ Arquivo tratado salvo em: {PROCESSED_FILE}")
    print(f"📦 Tamanho final: {size_gb:.2f} GB")
    print(f"⏱️ Tempo total de execução: {elapsed:.2f} minutos")

if __name__ == "__main__":
    main()
