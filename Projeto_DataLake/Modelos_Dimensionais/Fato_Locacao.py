from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

spark = SparkSession.builder.appName("GenerateFatoLocacao").getOrCreate()

# Caminhos das camadas
organization_path = "/mnt/datalake/organization/"
query_path = "/mnt/datalake/query/"

# Tabelas da camada de organização
tables = {
    'loc_pedido_locacao': f"{organization_path}loc_pedido_locacao",
    'loc_cliente': f"{organization_path}loc_cliente",
    'loc_veiculo': f"{organization_path}loc_veiculo",
    'loc_agencia': f"{organization_path}loc_agencia",
    'loc_cidade': f"{organization_path}loc_cidade",
    'loc_estado': f"{organization_path}loc_estado"
}

try:
    logger.info("Carregando tabelas organizadas...")
    pedido_locacao = spark.read.parquet(tables['loc_pedido_locacao'])
    cliente = spark.read.parquet(tables['loc_cliente'])
    veiculo = spark.read.parquet(tables['loc_veiculo'])
    agencia = spark.read.parquet(tables['loc_agencia'])
    cidade = spark.read.parquet(tables['loc_cidade'])
    estado = spark.read.parquet(tables['loc_estado'])

    # Criar Dim_Localizacao
    logger.info("Criando dimensão Dim_Localizacao...")
    dim_localizacao = cidade.join(
        estado.withColumnRenamed("cd_estado", "estado_cd_estado"),
        cidade['cd_estado'] == col("estado_cd_estado"),
        'left'
    ).select(
        col('cd_cidade').alias('id_cidade'),
        col('nm_cidade').alias('nome_cidade'),
        col('cd_estado').alias('id_estado'),
        col('nm_estado').alias('nome_estado'),
        col('sigla_estado')
    )
    dim_localizacao.write.mode("overwrite").parquet(f"{query_path}dim_localizacao")
    logger.info("Dim_Localizacao criada com sucesso.")

    # Criar Dim_Cliente
    logger.info("Criando dimensão Dim_Cliente...")
    dim_cliente = cliente.select(
        col('cd_cliente').alias('id_cliente'),
        col('nm_cliente').alias('nome_cliente'),
        col('cd_tp_cliente').alias('tipo_cliente'),
        col('nr_habilitacao').alias('numero_habilitacao')
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

    # Criar Dim_Agencia
    logger.info("Criando dimensão Dim_Agencia...")
    dim_agencia = agencia.select(
        col('cd_agencia').alias('id_agencia'),
        col('nm_agencia').alias('nome_agencia'),
        col('cd_cidade').alias('id_cidade')
    )
    dim_agencia.write.mode("overwrite").parquet(f"{query_path}dim_agencia")
    logger.info("Dim_Agencia criada com sucesso.")

    # Criar Fato_Locacao
    logger.info("Criando tabela fato Fato_Locacao...")
    fato_locacao = pedido_locacao.join(dim_cliente, pedido_locacao['cd_cliente'] == dim_cliente['id_cliente'], 'inner') \
        .join(dim_veiculo, pedido_locacao['nr_pedido'] == dim_veiculo['id_veiculo'], 'inner') \
        .join(dim_agencia, pedido_locacao['cd_agencia'] == dim_agencia['id_agencia'], 'inner') \
        .join(dim_localizacao, dim_agencia['id_cidade'] == dim_localizacao['id_cidade'], 'left') \
        .select(
            pedido_locacao['nr_pedido'].alias('id_locacao'),
            pedido_locacao['dt_locacao'].alias('data_locacao'),
            pedido_locacao['dt_entrega'].alias('data_entrega'),
            pedido_locacao['qt_dias'].alias('quantidade_dias'),
            pedido_locacao['vl_total'].alias('valor_total'),
            pedido_locacao['status'].alias('status_locacao'),
            dim_cliente['id_cliente'],
            dim_veiculo['id_veiculo'],
            dim_agencia['id_agencia'],
            dim_localizacao['id_cidade'],
            dim_localizacao['nome_cidade'],
            dim_localizacao['nome_estado'],
            dim_localizacao['sigla_estado']
        )

    fato_locacao.write.mode("overwrite").parquet(f"{query_path}fato_locacao")
    logger.info("Fato_Locacao criada com sucesso e salva na camada de query.")

except Exception as e:
    logger.error(f"Erro ao criar a tabela Fato_Locacao: {e}")



