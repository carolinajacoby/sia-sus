from pathlib import Path
import polars as pl
import time

# Caminhos principais
PROJETO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJETO_ROOT / "data/raw/AM/2024"  # onde estão os parquet baixados por UF
PROCESSED_DIR = PROJETO_ROOT / "data/processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = PROCESSED_DIR / "am_2024_consolidado_final.zstd.parquet"

def main():
    start_time = time.time()

    # Lista todos os arquivos .parquet nas subpastas
    parquet_files = list(RAW_DIR.rglob("*.parquet"))
    if not parquet_files:
        print(" Nenhum arquivo .parquet encontrado em:", RAW_DIR)
        return

    print(f"🔹 {len(parquet_files)} arquivos encontrados para concatenação.")

    # Cria LazyFrames (sem carregar tudo na memória)
    lazy_frames = []
    for i, f in enumerate(parquet_files, 1):
        try:
            print(f"[{i}/{len(parquet_files)}] Adicionando {f.name} ao pipeline (lazy)...")
            lazy_frames.append(pl.scan_parquet(f))
        except Exception as e:
            print(f"Erro ao ler {f.name}: {e}")

    if not lazy_frames:
        print(" Nenhum arquivo pôde ser carregado.")
        return

    # Concatenação preguiçosa
    combined_lazy = pl.concat(lazy_frames, how="vertical")
    print("Concatenação concluída (lazy). Salvando...")

    # Salva comprimido em ZSTD
    combined_lazy.sink_parquet(
        OUTPUT_FILE,
        compression="zstd",
        compression_level=7
    )

    elapsed = (time.time() - start_time) / 60
    size_gb = OUTPUT_FILE.stat().st_size / (1024 ** 3)

    print(f"\n Arquivo consolidado salvo em: {OUTPUT_FILE}")
    print(f" Total de arquivos processados: {len(parquet_files)}")
    print(f" Tamanho final: {size_gb:.2f} GB")
    print(f" Tempo total de execução: {elapsed:.2f} minutos")


if __name__ == "__main__":
    main()
