import pandas as pd
import pandera as pa
from pytz import timezone
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pandera.typing import DataFrame
from contrato import ContratosDadosDengue

def estrai_dados(dir_arquivo: str) -> pd.DataFrame:
    try:
        # Leitura do arquivo CSV
        dados = pd.read_csv(dir_arquivo, sep=';', encoding='utf-8', low_memory=False)
        print(f"Dados extraídos com sucesso de {dir_arquivo}")
        return dados
    except Exception as e:
        print(f"Erro ao extrair os dados: {e}")
        return pd.DataFrame()

def transforma_dados(dados: pd.DataFrame) -> DataFrame[ContratosDadosDengue]:
    try:
        # Limpando os nomes das colunas
        dados.columns = [col.strip().lower() for col in dados.columns]

        #Pegando as colunas relevantes 
        colunas_relevantes = [
            "id_agravo", "dt_invest", "dt_notific", "dt_nasc", "dt_sin_pri", "id_ocupa_n", "febre",
            "mialgia", "cefaleia", "exantema", "vomito", "nausea", "dor_costas",
            "conjuntvit", "artrite", "artralgia", "petequia_n", "leucopenia", "laco",
            "dor_retro", "diabetes", "hematolog", "hepatopat", "renal", "hipertensa",
            "auto_imune", "resul_soro", "resul_ns1", "resul_pcr_", "sorotipo", "hospitaliz", 
            "tpautocto", "coufinf", "comuninf", "classi_fin", "criterio", "evolucao", "dt_obito", "cs_sexo"
        ]
 
        dados_filtrados = dados[colunas_relevantes]

        df["dt_nasc"] = pd.to_datetime(df["dt_nasc"], format="%Y-%m-%d", errors='coerce')
        df["dt_notific"] = pd.to_datetime(df["dt_notific"], format="%Y-%m-%d", errors='coerce')
        df["dt_sin_pri"] = pd.to_datetime(df["dt_sin_pri"], format="%Y-%m-%d", errors='coerce')

                                                                                
        # Validação dos dados após transformação
        dados_validados = ContratosDadosDengue.validate(dados_filtrados)
        print("Dados transformados e validados com sucesso.")
        return dados_validados
    
    except KeyError as e:
        print(f"Erro: Coluna faltando - {e}")
        raise
    
    except pa.errors.SchemaError as e:
        print(f"Erro de validação nos dados: {e}")
        raise
    
    except Exception as e:
        print(f"Erro na transformação ou validação dos dados: {e}")
        raise

def carrega_dados(df: pd.DataFrame) -> None:
    load_dotenv(".env")  # Carrega as variáveis de ambiente

    try:
        # Pegando as variáveis de ambiente
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        POSTGRES_DB = os.getenv("POSTGRES_DB")

        # Verificando se todas as variáveis estão definidas
        if not all([POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB]):
            raise ValueError("Uma ou mais variáveis de ambiente não estão definidas.")

        POSTGRES_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

        # Criando o engine para conexão com o banco de dados
        engine = create_engine(POSTGRES_DATABASE_URL)

        nome_da_tabela = "dengue_casos"

        try:
            # Carregando os dados no banco de dados
            df.to_sql(nome_da_tabela, engine, if_exists="replace", index=False)
            print("Dados carregados com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar os dados no banco: {e}")

    except Exception as e:
        print(f"Erro com as variáveis de ambiente ou conexão com o banco: {e}")

if __name__ == '__main__':
    # Caminho do arquivo de dados
    dir_arquivo = "data/dengue_bahia_2020_2024.csv"
    df = estrai_dados(dir_arquivo)
    
    if not df.empty:
        # Transformando e validando os dados
        df_transformado = transforma_dados(df)
        
        if not df_transformado.empty:
            # Carregando os dados no banco
            carrega_dados(df_transformado)
        else:
            print("Dados transformados não são válidos.")
    else:
        print("Erro ao extrair os dados.")
