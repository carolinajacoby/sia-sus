from pysus import SIA
from pathlib import Path

def baixar_am_2024(uf_list=None):
    """
    Baixa todos os arquivos do grupo AM (APAC de Medicamentos) para o ano de 2024
    para os estados listados em uf_list (ou todos se None).
    """
    sia = SIA().load()  # Carrega o SIA

    if uf_list is None:
        uf_list = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
                   'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

    ano = 2024
    grupo = "AM"

    resultados = {}

    for uf in uf_list:
        print(f"\nBaixando arquivos do grupo {grupo} para {uf} no ano {ano}...")

        # Criar pasta destino
        pasta = Path(f"data/raw/{grupo}/{ano}/{uf}")
        pasta.mkdir(parents=True, exist_ok=True)

        # Pega os arquivos disponíveis para o estado e ano
        arquivos = sia.get_files(grupo, uf=uf, year=ano)
        resultados[uf] = arquivos

        if not arquivos:
            print(f"Nenhum arquivo encontrado para {uf}.")
            continue

        # Baixar os arquivos para a pasta
        sia.download(arquivos, local_dir=pasta)
        print(f"{uf}: {len(arquivos)} arquivos baixados para {pasta}")

    return resultados
