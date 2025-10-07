import time
from pathlib import Path
from extract.sia_extraction import baixar_am_2024
from transform import process_data
from transform import process_brazil

def main():
    start_time = time.time()

    # --- ETAPA 1: EXTRAÇÃO (DOWNLOAD) ---
    print("--- INICIANDO ETAPA DE EXTRAÇÃO DE DADOS ---")
    uf_list = None  # None = todas as UFs
    resultados = baixar_am_2024(uf_list=uf_list)

    print("\nResumo do download:")
    for uf, arquivos in resultados.items():
        print(f"{uf}: {len(arquivos)} arquivos baixados")

    # --- ETAPA 2: CONCATENAÇÃO E COMPRESSÃO (Polars) ---
    print("\n--- INICIANDO CONCATENAÇÃO E COMPRESSÃO COM POLARS ---")
    process_data.main()

    # --- ETAPA 3: TRATAMENTO DE TIPOS E DATAS (process_brazil) ---
    print("\n--- INICIANDO TRATAMENTO DE TIPOS E DATAS ---")
    process_brazil.main()

    # --- MÉTRICAS DE DESEMPENHO ---
    elapsed_time = (time.time() - start_time) / 60
    print(f"\n Tempo total de execução: {elapsed_time:.2f} minutos")

    # Arquivo final tratado
    processed_file = Path("data/processed/am_2024_tratado.zstd.parquet")
    if processed_file.exists():
        size_gb = processed_file.stat().st_size / (1024**3)
        print(f"Tamanho do arquivo final tratado: {size_gb:.2f} GB")

    print("\nFLUXO DE TRABALHO CONCLUÍDO COM SUCESSO")


if __name__ == "__main__":
    main()
