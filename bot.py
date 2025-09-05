import requests
import pandas as pd
import os
from typing import Any

# --- Constantes Globais ---
URL_IPCA_METADADOs = "https://sidra.ibge.gov.br/Ajax/Json/Tabela/1/1737?versao=-1"
PASTA_SAIDA = "saida_dados"


def capturar_dados_api(url: str) -> Any:
    """Realiza uma requisição GET para um endpoint da API e retorna a resposta JSON.

    Args:
        url (str): O endpoint da API a ser consultado.

    Returns:
        Any: Um dicionário ou lista contendo os dados da resposta JSON, ou None em caso de falha na requisição.
    """
    print(f"Buscando dados em: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Dados capturados com sucesso!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  -> Erro de conexão ao acessar a URL: {e}")
    except requests.exceptions.JSONDecodeError:
        print("  -> Erro: A resposta da API não é um JSON válido.")
    return None


def extrair_lista_de_metadados(dados_json: dict, caminho_chaves: list) -> pd.DataFrame:
    """Navega em uma estrutura de dicionário para extrair e formatar uma lista de metadados.

    Args:
        dados_json (dict): O dicionário fonte contendo os metadados.
        caminho_chaves (list): Uma lista de chaves que representa o caminho até a lista de dados desejada.

    Returns:
        pd.DataFrame: Um DataFrame com os dados extraídos, ou um DataFrame
                      vazio se o caminho for inválido ou a lista não for encontrada.
    """
    dados_atuais = dados_json
    for chave in caminho_chaves:
        dados_atuais = dados_atuais.get(chave)
        if dados_atuais is None:
            return pd.DataFrame()
    
    lista_de_dados = dados_atuais

    if not isinstance(lista_de_dados, list) or not lista_de_dados:
        return pd.DataFrame()

    if all(isinstance(item, str) for item in lista_de_dados):
        nome_coluna = caminho_chaves[-1].replace('s', '').capitalize() 
        return pd.DataFrame(lista_de_dados, columns=[nome_coluna])
    
    return pd.DataFrame(lista_de_dados)


def processar_dados_numericos(json_data: list) -> pd.DataFrame:
    """Transforma a resposta da API de dados numéricos em um DataFrame limpo.

    Args:
        json_data (list): A lista de dicionários retornada pela API.

    Returns:
        pd.DataFrame: Um DataFrame com os dados numéricos e colunas renomeadas, ou um DataFrame vazio se a entrada for inválida.
    """
    if not json_data or len(json_data) < 2:
        return pd.DataFrame()
    
    dados_reais = json_data[1:]
    df = pd.DataFrame(dados_reais)
    
    nomes_colunas = json_data[0]
    df = df.rename(columns=nomes_colunas)
    return df


def gravar_arquivo_parquet(df: pd.DataFrame, nome_arquivo: str, pasta: str):
    """Persiste um DataFrame em disco no formato Parquet.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        nome_arquivo (str): O nome do arquivo de destino.
        pasta (str): O diretório onde o arquivo será salvo.
    """
    if df.empty:
        print(f"DataFrame para '{nome_arquivo}' está vazio. Arquivo não gerado.")
        return

    caminho_completo = os.path.join(pasta, nome_arquivo)
    try:
        df.to_parquet(caminho_completo, engine='pyarrow', index=False)
        print(f"Arquivo '{caminho_completo}' salvo com sucesso!")
    except Exception as e:
        print(f"Ocorreu um erro ao salvar o arquivo '{caminho_completo}': {e}")


def orquestrar_captura_metadados() -> dict | None:
    """Orquestra a Etapa 1 do processo: captura e armazenamento dos metadados.

    Returns:
        dict | None: O dicionário JSON de metadados se a operação for bem-sucedida, caso contrário, None.
    """
    print("\n--- Etapa 1: CAPTURANDO METADADOS DA TABELA ---")
    dados_json_meta = capturar_dados_api(URL_IPCA_METADADOs)

    if not dados_json_meta:
        return None

    tabelas_para_extrair = {
        'variaveis.parquet': ['Variaveis'],
        'unidades_de_medida.parquet': ['UnidadesDeMedida'],
        'periodos.parquet': ['Periodos', 'Periodos'],
        'conjuntos_periodos.parquet': ['Periodos', 'Conjuntos'],
        'notas.parquet': ['Notas']
    }

    for nome_arquivo, caminho in tabelas_para_extrair.items():
        df_processado = extrair_lista_de_metadados(dados_json_meta, caminho)
        gravar_arquivo_parquet(df_processado, nome_arquivo, PASTA_SAIDA)
        print("---")
    
    return dados_json_meta


def orquestrar_captura_dados_numericos(dados_metadados: dict):
    """Orquestra a Etapa 2: captura dos dados numéricos para cada variável.

    Args:
        dados_metadados (dict): O dicionário de metadados obtido na Etapa 1.
    """
    print("\n--- Etapa 2: CAPTURANDO DADOS NUMÉRICOS DAS VARIÁVEIS ---")
    caminho_variaveis = os.path.join(PASTA_SAIDA, 'variaveis.parquet')
    
    try:
        df_variaveis = pd.read_parquet(caminho_variaveis)
        print(f"Arquivo de variáveis lido com sucesso de: {caminho_variaveis}")
    except FileNotFoundError:
        print(f"Erro: Arquivo '{caminho_variaveis}' não encontrado. A Etapa 1 pode ter falhado.")
        return
        
    id_tabela = dados_metadados.get('Id', '1737')

    for _, variavel in df_variaveis.iterrows():
        var_id = variavel['Id']
        var_decimais = variavel['DecimaisApresentacao']
        var_nome = variavel['Nome']
        
        print(f"\nBuscando dados para a variável: '{var_nome}' (ID: {var_id})")
        url_dados = f"https://apisidra.ibge.gov.br/values/t/{id_tabela}/n1/all/v/{var_id}/p/all/d/v{var_id}%20{var_decimais}"
        
        dados_numericos_json = capturar_dados_api(url_dados)
        
        if dados_numericos_json:
            df_dados_numericos = processar_dados_numericos(dados_numericos_json)
            nome_arquivo_dados = f"dados_numericos_{var_id}.parquet"
            gravar_arquivo_parquet(df_dados_numericos, nome_arquivo_dados, PASTA_SAIDA)
        print("---")


def main():
    """Ponto de entrada principal do script.

    Executa o pipeline de captura de dados em duas Etapas:
    1 - Coleta de metadados da tabela do IBGE.
    2 - Coleta de dados numéricos com base nos metadados da Etapa anterior.
    """
    print("--- INÍCIO DO PROCESSO DO BOT ---")
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    metadados = orquestrar_captura_metadados()

    if metadados:
        orquestrar_captura_dados_numericos(metadados)
    else:
        print("Etapa 2 não pode ser executada pois a Etapa 1 falhou.")

    print("\n--- FIM DO PROCESSO DO BOT ---")


if __name__ == "__main__":
    main()