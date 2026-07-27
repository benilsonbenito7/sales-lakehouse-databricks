from bronze_utils import ler_csv, salvar_em_delta
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def main():
    #Fase ler o csv onde estao as informacoes originais
    df_bronze = ler_csv(spark)
    #Ingestao dos dados brutos do arquivo csv e armazena-los em uma tabela delta, preservando assim o conteudo original dos dados para o processamento posterior.
    caminho = f"retail_sales.bronze.sales_raw"
    df_bronze = salvar_em_delta(df_bronze, caminho)
    
if __name__ == "__main__":
    main()