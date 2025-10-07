### Projeto SIA-SUS: Extração e Processamento de Dados AM 2024

Este projeto realiza a extração, concatenação e processamento de dados do grupo AM (APAC de Medicamentos) do SUS para o ano de 2024. O objetivo é demonstrar boas práticas de ETL, manipulação eficiente de grandes volumes de dados e refatoração de código para performance, utilizando o Polars para processamento em paralelo.

## Estrutura  do  projeto

sia-sus/
├── extract/
│   └── sia_extraction.py       # Extração de dados do SIA via PySUS
├── transform/
│   ├── process_data.py         # Concatenação e merge inicial com Polars LazyFrame
│   └── process_brazil.py       # Refatoração e tipagem (Colunas, Datas, Compressão ZSTD)
├── data/
│   ├── raw/                    # Arquivos .dbc brutos baixados (AM/2024/<UF>/)
│   └── processed/              # Arquivo Parquet consolidado e tratado
├── main.py                     # Orquestração completa do fluxo (ETL)
└── requirements.txt            # Dependências do projeto


## Fluxo do trabalho

## 1- Extração de dados

Baixa arquivos do grupo AM (APAC de Medicamentos) para todas as UFs do Brasil usando pysus. e salva  na  pasta data/raw/AM/2024/<UF>/

# Como executar

# Execução completa do ETL
python3 main.py

# Este script orquestra os passos 1, 2 e 3 em sequência.

## 2 - Concatenação dos arquivos

Lê todos os arquivos baixados anteriormente e concatena em um único arquivo parquet.
Usei polars e lazyframe par facilitar performance. Salva o arquivo concatenado  na pasta data/processed.

# Como executar

python3 transform/process_data.py

## 3 - Refatoração e tratamento de dados

Renomeia as colunas para  nomes mais simples, aplica tipo de dado correto(nos dados  brutos todas estão como tipo string). Salva o arquivo Parquet final usando compressão ZSTD afim de melhorar a performance de I/O. O arquivo final, com aproximadamente 35 milhões de linhas, está pronto para análises futuras e apresenta um tamanho reduzido de cerca de ~1.1 GB.

### Dicionário dos dados orignal SIA-SUS

[Dicionário de Dados](https://github.com/carolinajacoby/sia-sus/blob/main/docs/Dicionario_Dados.md)

## Mapeamento de Campos de Dados (Original vs. Descritivo)

| Nome Original | Nome Atualizado/Descritivo |
| :--- | :--- |
| **AP_MVM** | data\_movimento |
| **AP_CONDIC** | tipo\_gestao |
| **AP_GESTAO** | codigo\_uf\_municipio\_gestao |
| **AP_CODUNI** | codigo\_estabelecimento |
| **AP_AUTORIZ** | numero\_apac |
| **AP_CMP** | data\_atendimento |
| **AP_PRIPAL** | procedimento\_principal |
| **AP_VL_AP** | valor\_apac |
| **AP_UFMUN** | uf\_municipio\_estabelecimento |
| **AP_TPUPS** | tipo\_estabelecimento |
| **AP_TIPPRE** | tipo\_prestador |
| **AP_MN_IND** | manutencao\_individual |
| **AP_CNPJCPF** | cnpj\_estabelecimento |
| **AP_CNPJMNT** | cnpj\_mantenedora |
| **AP_CNSPCN** | cns\_paciente |
| **AP_COIDADE** | codigo\_idade |
| **AP_NUIDADE** | idade |
| **AP_SEXO** | sexo |
| **AP_RACACOR** | raca\_cor |
| **AP_MUNPCN** | uf\_municipio\_residencia |
| **AP_UFNACIO** | codigo\_nacionalidade |
| **AP_CEPPCN** | cep |
| **AP_UFDIF** | uf\_diferente |
| **AP_MNDIF** | municipio\_diferente |
| **AP_DTINIC** | data\_inicio |
| **AP_DTFIM** | data\_fim |
| **AP_TPATEN** | tipo\_atendimento |
| **AP_TPAPAC** | indicador\_apac |
| **AP_MOTSAI** | motivo\_saida |
| **AP_OBITO** | obito |
| **AP_ENCERR** | encerramento |
| **AP_PERMAN** | permanencia |
| **AP_ALTA** | alta |
| **AP_TRANSF** | transferencia |
| **AP_DTOCOR** | data\_ocorrencia |
| **AP_CODEMI** | codigo\_orgao\_emissor |
| **AP_CATEND** | carater\_atendimento |
| **AP_APACANT** | numero\_apac\_anterior |
| **AP_UNISOL** | estabelecimento\_solicitante |
| **AP_DTSOLIC** | data\_solicitacao |
| **AP_DTAUT** | data\_autorizacao |
| **AP_CIDCAS** | cid\_causas\_associadas |
| **AP_CIDPRI** | cid\_principal |
| **AP_CIDSEC** | cid\_secundario |
| **AP_ETNIA** | etnia |
| **AM_PESO** | peso |
| **AM_ALTURA** | altura |
| **AM_TRANSPL** | transplante |
| **AM_QTDTRAN** | quantidade\_transplantes |
| **AM_GESTANT** | gestante |


# Como executar

python3 transform/process_brazil.py

============================================================

### Instalação e  dependências

## 1 - Ambiente virtual

Criar e ativar ambiente virtual do python

# Como executar

python3 -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

### 2 - Instalar dependências

pip install -r requirements.txt

## Principais dependências

[Python 3.12](https://www.python.org/downloads/)
[Polars](https://www.pola.rs/)
[PySus](https://pysus.readthedocs.io/)

============================================================

---
## Licença

Este projeto está licenciado sob a Licença MIT.

