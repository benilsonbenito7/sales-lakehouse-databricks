from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame

def vendas_por_mes(df: DataFrame) -> DataFrame:
    df = df.groupBy('ano') \
        .agg(
            round(sum(col("vendas_calculado")), 2).alias("vendas_total"),
            sum(col("quantidade_pedida")).alias("quantidade_total"),
            count('*').alias("linhas")
        ) \
        .orderBy(col("ano").desc())

    return df

def vendas_por_status(df: DataFrame) -> DataFrame:
    df = df.groupBy('status_pedido') \
        .agg(
            round(sum(col("vendas_calculado")), 2).alias("vendas_total"),
            sum(col("quantidade_pedida")).alias("quantidade_total"),
            count('*').alias('total_pedidos'),
            ) \
        .orderBy(col("total_pedidos").desc())

    return df

def top_vendas(df: DataFrame ) -> DataFrame:
    df = df.groupBy("ano", "mes") \
        .agg(
            countDistinct("numero_pedido").alias("top_pedidos"),
            round(sum(col("vendas_calculado")), 2).alias("vendas_total"),
            sum(col("quantidade_pedida")).alias("quantidade_total"),
            count("numero_linha_pedido").alias("linhas"),
    ).orderBy(col("top_pedidos").desc())
        
    return df

def salvar_as_tabelas(df_vendas_por_ano: DataFrame, df_vendas_por_status: DataFrame, df_top_vendas: DataFrame) ->None:
    df_vendas_por_ano.write.mode("overwrite").saveAsTable("vendas_ano")
    df_vendas_por_status.write.mode("overwrite").saveAsTable("vendas_status")
    df_top_vendas.write.mode("overwrite").saveAsTable("top_vendas_")

