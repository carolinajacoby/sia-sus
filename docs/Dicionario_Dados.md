# Dicionário de Dados – SIA/APAC + AM

Este dicionário traz os campos do SIA/APAC e AM, com nomes simplificados para uso em ETL e análise.  

---

## Identificação e Processamento
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_MVM         | data_processamento | Data de Processamento / Movimento (AAAAMM) |
| AP_CONDIC      | tipo_gestao        | Sigla do Tipo de Gestão |
| AP_GESTAO      | cod_mun_gestao     | Código município de gestão / UF0000 se estadual |
| AP_CODUNI      | codigo_cnes        | Código CNES do Estabelecimento executante |
| AP_AUTORIZ     | numero_apac        | Número da APAC |
| AP_CMP         | data_atendimento   | Data de atendimento (AAAAMM) |
| AP_PRIPAL      | procedimento_principal | Procedimento principal da APAC |
| AP_VL_AP       | valor_apac         | Valor total da APAC aprovado |

---

## Estabelecimento e Prestador
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_UFMUN       | cod_mun_exec          | Código IBGE do município do estabelecimento |
| AP_TPUPS       | tipo_estabelecimento  | Tipo de Estabelecimento |
| AP_TIPPRE      | tipo_prestador        | Tipo de Prestador |
| AP_MN_IND      | natureza_prestador    | Estabelecimento mantido / individual |
| AP_CNPJCPF     | cnpj_estabelecimento  | CNPJ/CPF do estabelecimento executante |
| AP_CNPJMNT     | cnpj_mantenedora      | CNPJ da mantenedora (ou zeros) |

---

## Paciente
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_CNSPCN      | cns_paciente        | CNS do paciente |
| AP_MUNPCN      | cod_mun_res         | Código IBGE município de residência |
| AP_UFNACIO     | cod_nacionalidade   | Nacionalidade do paciente |
| AP_CEPPCN      | cep_paciente        | CEP do paciente |
| AP_UFDIF       | uf_diferente        | UF residência ≠ UF estabelecimento |
| AP_MNDIF       | mun_diferente       | Município residência ≠ município estabelecimento |
| AP_COIDADE     | tipo_idade          | Tipo da idade (anos/meses/dias) |
| AP_NUIDADE     | idade               | Idade do paciente |
| AP_SEXO        | sexo_paciente       | Sexo do paciente |
| AP_RACACOR     | raca_cor_paciente   | Raça/cor do paciente |
| AP_ETNIA       | etnia_paciente      | Etnia do paciente |
| AM_PESO        | peso_paciente       | Peso (kg) |
| AM_ALTURA      | altura_paciente     | Altura (cm) |
| AM_TRANSPL     | ind_transplante     | Indicador transplante |
| AM_QTDTRAN     | qtd_transplantes    | Quantidade de transplantes |
| AM_GESTANT     | ind_gestante        | Indicador gestante |

---

## APAC – Ciclo e Validade
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_DTINIC      | data_inicio        | Data início validade |
| AP_DTFIM       | data_fim           | Data fim validade |
| AP_TPATEN      | tipo_atendimento   | Tipo de atendimento da APAC |
| AP_TPAPAC      | tipo_apac          | Tipo de APAC (inicial/continuidade/única) |
| AP_APACANT     | numero_apac_anterior | Número da APAC anterior |
| AP_DTOCOR      | data_ocorrencia    | Data de ocorrência (substitui fim validade) |

---

## Condições de Saída / Alta
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_MOTSAI      | motivo_saida        | Motivo de saída/permanência |
| AP_OBITO       | ind_obito           | Indicador óbito |
| AP_ENCERR      | ind_encerramento    | Indicador encerramento |
| AP_PERMAN      | ind_permanencia     | Indicador permanência |
| AP_ALTA        | ind_alta            | Indicador alta |
| AP_TRANSF      | ind_transferencia   | Indicador transferência |

---

## Solicitação e Autorização
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_CODEMI      | cod_orgao_emissor     | Código órgão emissor |
| AP_CATEND      | caracter_atendimento  | Caráter do atendimento |
| AP_UNISOL      | cod_cnes_solicitante  | CNES do estabelecimento solicitante |
| AP_DTSOLIC     | data_solicitacao      | Data da solicitação |
| AP_DTAUT       | data_autorizacao      | Data da autorização |

---

## Diagnósticos
| Campo original | Nome simplificado | Descrição |
|----------------|-----------------|-----------|
| AP_CIDCAS      | cid_causa_associada | CID causas associadas |
| AP_CIDPRI      | cid_principal       | CID principal |
| AP_CIDSEC      | cid_secundario     | CID secundário |
