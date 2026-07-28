from pyspark.sql import DataFrame
from monitoramento.monitoramento import registrar_info, registrar_warning, registrar_error, CorTerminal

def ler_csv(spark) -> DataFrame:
    """Lê o arquivo CSV da camada Bronze e retorna um DataFrame."""
    registrar_info("Iniciando leitura do arquivo csv")
    
    arquivo = f"/Volumes/retail_sales/bronze/volume_raw/sales_data_sample-selected-columns(1).csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", True).option("encoding", "UTF-8").load(arquivo)
    registrar_info(f"Arquivo csv carregado com sucesso e possui {CorTerminal.VERDE}{df.count()}{CorTerminal.RESET} registros")
    
    return df

def salvar_em_delta(df: DataFrame, caminho: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(caminho)
    registrar_info(f"Tabela delta [sales_brute] salva e criada com sucesso")


