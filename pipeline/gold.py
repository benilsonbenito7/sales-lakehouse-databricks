from gold_utils import *
from monitoramento.monitoramento import *
import time

def main():
    try:
        registrar_info("Iniciando camada Gold")
        inicio = time.perf_counter()

        df_silver = spark.read.table("retail_sales.silver.clean_sales")
        registrar_info(f"Dados da tabela [clean_sales] lidos com {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")

        #criando a primeira regra de negocio
        df_vendas_por_ano = vendas_por_mes(df_silver)
        #df_vendas_por_ano.show()
        registrar_info(f"Regra de negocio 1: {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")

        #criando a segunda regra de negocio
        df_vendas_por_status = vendas_por_status(df_silver)
        #df_vendas_por_status.show()
        registrar_info(f"Regra de negocio 2: {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")

        #criando a terceira regra de negocio
        df_top_vendas = top_vendas(df_silver)
        #df_top_vendas.show()

        registrar_info(f"Regra de negocio 3: {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")
        salvar_as_tabelas(df_vendas_por_ano, df_vendas_por_status, df_top_vendas)
        registrar_info(f"Salvando tabelas: {CorTerminal.VERDE}sucesso{CorTerminal.RESET}")

        fim = time.perf_counter()
        registrar_info(f"Camada Gold Finalizado Tempo de execução: {CorTerminal.VERDE}{fim - inicio:.2f}{CorTerminal.RESET} segundos")
    except Exception as e:
        registrar_error(f"Erro: {e}")


if __name__ == "__main__":
    main()