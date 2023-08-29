# Databricks notebook source
import requests
from pyspark.sql.functions import lit

# COMMAND ----------


def extraindo_dados(date, base):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"
    print(url)

    headers = {
    "apikey": "your here"
    }

    parametros = {"base":base}

    response = requests.request("GET", url, headers=headers, params=parametros)

    print(response.text)
    if response.status_code != 200:
        raise Exception("data not extracted !!!")

    status_code= response.status_code
    return response.json()


# COMMAND ----------

def dados_para_dataframe (dado_json): 
    dados_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
    return dados_tupla

# COMMAND ----------

def salvar_arquivo_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split("-")

    caminho_completo = f"/databricks-results/bronze/{ano}/{mes}/{dia}"
    result = dados_para_dataframe(conversoes_extraidas)

    df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])

    df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))

    df_conversoes.write.format("parquet").mode("overwrite").save(caminho_completo)

    print(f"Arquivos salvos em {caminho_completo}")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-results/bronze/2023/01/01

# COMMAND ----------

cotacoes = extraindo_dados('2023-01-01', "EUR")
salvar_arquivo_parquet(cotacoes)
