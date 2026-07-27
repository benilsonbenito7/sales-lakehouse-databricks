from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def ler_csv(spark) -> DataFrame:
    """Lê o arquivo CSV da camada Bronze e retorna um DataFrame."""
    df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("/Volumes/retail_sales/bronze/volume_raw/sales_data_sample-selected-columns(1).csv")
    return df

def salvar_em_delta(df: DataFrame, caminho: str):
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(caminho)
    print(f"Tabela {caminho} salva com sucesso")


