from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, sum
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

spark = SparkSession.builder.appName("GenerateFatoReceita").getOrCreate()

# Caminhos das camadas
organization_path = "/mnt/datalake/organization/"
query_path = "/mnt/datalake/query/"

# Tabelas da camada de organização
tables = {
    'loc_pedido_locacao': f"{organization_path}loc_pedido_locacao",
    'loc_cliente': f"{organization_path}loc_cliente",
    'loc_veiculo': f"{organization_path}loc_veiculo",
    'loc_tp_cliente': f"{organization_path}loc_tp_cliente"
}

try:
    logger.info("Carregando tabelas organizadas...")
    pedido_locacao = spark.read.parquet(tables['loc_pedido_locacao'])
    cliente = spark.read.parquet(tables['loc_cliente'])
    veiculo = spark.read.parquet(tables['loc_veiculo'])
    tipo_cliente = spark.read.parquet(tables['loc_tp_cliente'])

    # Criar Dim_Tempo
    logger.info("Criando dimensão Dim_Tempo...")
    dim_tempo = pedido_locacao.select(
        col('dt_locacao').alias('data'),
        year(col('dt_locacao')).alias('ano'),
        month(col('dt_locacao')).alias('mes')
    ).dropDuplicates()
    dim_tempo.write.mode("overwrite").parquet(f"{query_path}dim_tempo")
    logger.info("Dim_Tempo criada com sucesso.")

    # Criar Dim_Cliente
    logger.info("Criando dimensão Dim_Cliente...")
    dim_cliente = cliente.join(tipo_cliente, cliente['cd_tp_cliente'] == tipo_cliente['cd_tp_cliente'], 'left') \
        .select(
            col('cd_cliente').alias('id_cliente'),
            col('nm_cliente').alias('nome_cliente'),
            col('nm_tp_cliente').alias('tipo_cliente')
        )
    dim_cliente.write.mode("overwrite").parquet(f"{query_path}dim_cliente")
    logger.info("Dim_Cliente criada com sucesso.")

    # Criar Dim_Veiculo
    logger.info("Criando dimensão Dim_Veiculo...")
    dim_veiculo = veiculo.select(
        col('nr_placa').alias('id_veiculo'),
        col('cd_modelo').alias('id_modelo'),
        col('tp_automovel').alias('tipo_automovel'),
        col('tp_combustivel').alias('tipo_combustivel')
    )
    dim_veiculo.write.mode("overwrite").parquet(f"{query_path}dim_veiculo")
    logger.info("Dim_Veiculo criada com sucesso.")

    # Criar Fato_Receita
    logger.info("Criando tabela fato Fato_Receita...")
    fato_receita = pedido_locacao.join(dim_cliente, pedido_locacao['cd_cliente'] == dim_cliente['id_cliente'], 'inner') \
        .join(dim_veiculo, pedido_locacao['nr_pedido'] == dim_veiculo['id_veiculo'], 'inner') \
        .select(
            pedido_locacao['nr_pedido'].alias('id_receita'),
            pedido_locacao['dt_locacao'].alias('data_locacao'),
            pedido_locacao['qt_dias'].alias('quantidade_dias'),
            pedido_locacao['vl_total'].alias('valor_total'),
            dim_cliente['id_cliente'],
            dim_veiculo['id_veiculo']
        )

    # Calcular receita total por cliente e veículo
    fato_receita = fato_receita.groupBy('id_cliente', 'id_veiculo').agg(
        sum('valor_total').alias('receita_total'),
        sum('quantidade_dias').alias('dias_totais')
    )

    fato_receita.write.mode("overwrite").parquet(f"{query_path}fato_receita")
    logger.info("Fato_Receita criada com sucesso e salva na camada de query.")

except Exception as e:
    logger.error(f"Erro ao criar a tabela Fato_Receita: {e}")

