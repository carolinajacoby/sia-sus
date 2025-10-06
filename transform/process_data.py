from pathlib import Path
import pandas as pd
import os 

# Define o caminho raiz do projeto subindo um nível a partir da pasta 'transform'
PROJETO_ROOT = Path(__file__).resolve().parent.parent 

# Caminhos são definidos a partir desta raiz
RAW_DIR = PROJETO_ROOT / "data/raw/AM/2024" 
PROCESSED_DIR = PROJETO_ROOT / "data/processed" 
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def main():
    all_dfs = []

    print(f"DEBUG: Caminho Absoluto de Busca: {RAW_DIR.resolve()}")
    print("\n--- INICIANDO LEITURA E UNIFICAÇÃO DOS ARQUIVOS PARQUET ---")
    
    # 1. BUSCA: Encontra todos os arquivos .parquet nas subpastas
    arquivos_encontrados = list(RAW_DIR.rglob("*.parquet"))
    
    if not arquivos_encontrados:
        print("ERRO: NENHUM arquivo .parquet encontrado. Verifique a pasta de download.")
        return

    # 2. LEITURA: Itera e lê cada arquivo Parquet
    for i, parquet_file in enumerate(arquivos_encontrados):
        try:
            print(f"[{i+1}/{len(arquivos_encontrados)}] Lendo {parquet_file.name}...")
            
            df = pd.read_parquet(parquet_file) 
            all_dfs.append(df)
            
            print(f"  -> {len(df):,} linhas lidas.")
            
        except Exception as e:
            print(f"ERRO ao ler o arquivo Parquet {parquet_file.name}: {e}")

    if all_dfs:
        # 3. CONCATENAÇÃO E SALVAMENTO
        print("\n--- CONCATENANDO E SALVANDO O ARQUIVO FINAL ---")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        output_file = PROCESSED_DIR / "am_2024_consolidado_final.parquet" 
        
        # O Parquet é ideal para esta etapa!
        combined_df.to_parquet(output_file, index=False, engine='pyarrow') 
        print(f"\nSUCESSO! Todos os {len(combined_df):,} registros unidos e salvos em: {output_file.resolve()}")
    else:
        print("Nenhum arquivo Parquet válido foi processado com sucesso.")

if __name__ == "__main__":
    main()