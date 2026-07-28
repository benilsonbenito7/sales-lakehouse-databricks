from bronze_utils import ler_csv, salvar_em_delta
from pyspark.sql import SparkSession
from monitoramento.monitoramento import registrar_info, registrar_warning, registrar_error, CorTerminal

spark = SparkSession.builder.getOrCreate()

def main():
    registrar_info(f"Inicializando a camada bronze.....")
    try:
        #Fase ler o csv onde estao as informacoes originais
        df_bronze = ler_csv(spark)
        #Ingestao dos dados brutos do arquivo csv e armazena-los em uma tabela delta, preservando assim o conteudo original dos dados para o processamento posterior.
        caminho = f"retail_sales.bronze.sales_brute"
        salvar_em_delta(df_bronze, caminho)
        registrar_info(f"Camada bronze finalizada com {CorTerminal.VERDE}sucesso!{CorTerminal.RESET}")
    except Exception as e:
        registrar_error(f"Erro: {CorTerminal.VERMELHO}{e}{CorTerminal.RESET}")

if __name__ == "__main__":
    main()