from extract.sia_extraction import baixar_am_2024
from transform import process_data  

def main():
    # --- ETAPA 1: EXTRAÇÃO (DOWNLOAD) ---
    print("--- INICIANDO ETAPA DE EXTRAÇÃO DE DADOS ---")
    
    uf_list = None  # None = todas as UFs
    resultados = baixar_am_2024(uf_list=uf_list)

    print("\nResumo do download:")
    for uf, arquivos in resultados.items():
        print(f"{uf}: {len(arquivos)} arquivos baixados")

    # --- ETAPA 2: PROCESSAMENTO (CONVERSÃO PARA PARQUET) ---
    print("\n--- INICIANDO PROCESSAMENTO DOS ARQUIVOS PARA PARQUET ---")
    
    # Chama a função 'main' definida dentro do módulo 'process_data'.
    # Ela irá ler os arquivos DBC na pasta data/raw/AM/2024 e salvar o Parquet.
    process_data.main()

    print("\n--- FLUXO DE TRABALHO CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()