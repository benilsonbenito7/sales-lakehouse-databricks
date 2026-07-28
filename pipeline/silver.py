from silver_utils import *
from pyspark.sql import SparkSession
from monitoramento.monitoramento import *

def main():
    try:
        registrar_info(f"Camada Silver Inicializando....")
        #ler os dados da tabela delta da bronze
        registrar_info(f"Camada Silver Lendo dados da tabela bronze....")
        df_bronze = spark.read.table("retail_sales.bronze.sales_brute")
        registrar_info(f"Dados lidos com sucesso {df_bronze.columns} totalizando {CorTerminal.VERDE}{df_bronze.count()}{CorTerminal.RESET} registros")
        #Fase 3: Corrugir o schema
        df_schema = corrugir_schema(df_bronze)
        #Fase 4: Tratar Nullos
        df_nullos = tratar_nullos(df_schema)
        #Fase 5: tratar duplicados
        df_duplicados = tratar_duplicados(df_nullos)
        #Fase 6: padronizar os dados
        df_padronizar = padronizar_dados(df_duplicados)
        #Fase 7: derivar campos
        df_derivados = derivar_campos(df_padronizar)
        #Fase 8: renomear campos
        df_renomeados = renomear_campos(df_derivados)
        #Fase 9: gravar dados na tabela silver
        df_silver = df_renomeados
        gravar_dados_silver(df_silver)
        

    except Exception as e:
        registrar_error(f"Erro: {CorTerminal.VERMELHO}{e}{CorTerminal.RESET}")



if __name__ == "__main__":
    main()