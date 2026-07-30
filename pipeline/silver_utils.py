from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import builtins
from datetime import datetime
from monitoramento.monitoramento import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

def corrugir_schema(df: DataFrame) -> DataFrame:
    data = to_date(col("ORDERDATE"), "M/d/yyyy H:mm")
    df = df.withColumns({
        'ORDERDATE': data,
        'YEAR_ID': year(data),
        'MONTH_ID': month(data)
    })
    registrar_info("Schema Corrigido")
    return df
    
def tratar_nullos(df: DataFrame) -> DataFrame:
    df = df.dropna(subset=['ORDERNUMBER'])
    registrar_info(f"Dados nullos tratados com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
    return df

def tratar_duplicados(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["ORDERNUMBER", "ORDERLINENUMBER"])
    registrar_info(f"Dados duplicados tratados com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
    return df

def padronizar_dados(df: DataFrame) -> DataFrame:
    df = df.withColumns({
        'QTR_ID': quarter(col("ORDERDATE")),
        'STATUS': when( upper(col("STATUS")) == "SHIPPED", "ENVIADO")
        .when( upper(col("STATUS")) == "CANCELLED", "CANCELADO")
        .when( upper(col("STATUS")) == "IN PROCESS", "EM PROCESSAMENTO")
        .when( upper(col("STATUS")) == "RESOLVED", "RESOLVIDO")
        .when( upper(col("STATUS")) == "DISPUTED", "EM DISPUTA")
        .when( upper(col("STATUS")) == "ON HOLD", "EM ESPERA")
        .otherwise(col("STATUS"))
    })
    registrar_info(f"Dados padronizados com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
    return df

def derivar_campos(df: DataFrame) -> DataFrame:
    df = df.withColumns({
        'SALES_CALCULADO': round(col('QUANTITYORDERED') * col('PRICEEACH'), 2)
    })
    registrar_info(f"Campos derivados com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
    return df

def renomear_campos(df: DataFrame) ->DataFrame:
    df = df.withColumnsRenamed({
        "ORDERNUMBER": "numero_pedido",
        "ORDERLINENUMBER": "numero_linha_pedido",
        "ORDERDATE": "data_pedido",
        "YEAR_ID": "ano",
        "QTR_ID": "trimestre",
        "MONTH_ID": "mes",
        "STATUS": "status_pedido",
        "QUANTITYORDERED": "quantidade_pedida",
        "PRICEEACH": "preco_unitario",
        "SALES": "vendas",
        "SALES_CALCULADO": "vendas_calculado",
        })
    registrar_info(f"Campos renomeados com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
    return df

def gravar_dados_silver(df: DataFrame) ->None:
    tabela = f"retail_sales.silver.clean_sales"

    if not spark.catalog.tableExists(tabela):
        registrar_info(f"Tabela [{tabela}] não encontrada. Criando primeira versão da camada Silver.")
        df.write.format("delta").mode("overwrite").saveAsTable(tabela)
        registrar_info(f"Tabela [{tabela}] criada com sucesso.")

    else:
        registrar_info(f"Tabela [{tabela}] encontrada. Iniciando carga incremental (MERGE).")
        if df.schema != spark.table(tabela).schema:
            registrar_error(f"Erro schema alterado -> retornou um schema diferente do esperado.")
        
        tabela_delta = DeltaTable.forName(spark, tabela)
        registrar_info("Executando MERGE entre os dados de origem e a tabela Silver.")

        tabela_delta.alias("destino") \
            .merge(
                df.alias("origem"), 
                "destino.numero_pedido = origem.numero_pedido AND destino.numero_linha_pedido = origem.numero_linha_pedido"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        registrar_info(f"Carga incremental concluída com sucesso na tabela [{tabela}].")
        

    
            
    
        
