from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

spark = SparkSession.builder.appName("GenerateFatoDesempenhoVeiculo").getOrCreate()

# Caminhos das camadas
organization_path = "/mnt/datalake/organization/"
query_path = "/mnt/datalake/query/"

# Tabelas da camada de organização
tables = {
    'loc_pedido_locacao': f"{organization_path}loc_pedido_locacao",
    'loc_veiculo': f"{organization_path}loc_veiculo",
    'loc_modelo': f"{organization_path}loc_modelo",
    'loc_agencia': f"{organization_path}loc_agencia",
    'loc_cidade': f"{organization_path}loc_cidade",
    'loc_estado': f"{organization_path}loc_estado",
    'loc_fabricante': f"{organization_path}loc_fabricante"
}

try:
    logger.info("Carregando tabelas organizadas...")
    pedido_locacao = spark.read.parquet(tables['loc_pedido_locacao'])
    veiculo = spark.read.parquet(tables['loc_veiculo']).withColumnRenamed("cd_Modelo", "veiculo_cd_modelo")  # Correção aqui
    modelo = spark.read.parquet(tables['loc_modelo']).withColumnRenamed("cd_Modelo", "modelo_cd_modelo").withColumnRenamed("nm_modelo", "modelo_nome")
    agencia = spark.read.parquet(tables['loc_agencia']).withColumnRenamed("cd_cidade", "agencia_cd_cidade").withColumnRenamed("cd_estado", "agencia_cd_estado")
    cidade = spark.read.parquet(tables['loc_cidade']).withColumnRenamed("cd_estado", "cidade_cd_estado").withColumnRenamed("cd_cidade", "cidade_cd_cidade")
    estado = spark.read.parquet(tables['loc_estado']).withColumnRenamed("cd_estado", "estado_cd_estado")
    fabricante = spark.read.parquet(tables['loc_fabricante']).withColumnRenamed("cd_fabricante", "fabricante_cd_fabricante")

    # Criar Dim_Localizacao
    logger.info("Criando dimensão Dim_Localizacao...")
    dim_localizacao = cidade.join(
        estado,
        cidade['cidade_cd_estado'] == estado['estado_cd_estado'],
        'left'
    ).select(
        col('cidade_cd_cidade').alias('id_cidade'),
        col('nm_cidade').alias('nome_cidade'),
        col('estado_cd_estado').alias('id_estado'),
        col('nm_estado').alias('nome_estado'),
        col('sigla_estado')
    )
    dim_localizacao.write.mode("overwrite").parquet(f"{query_path}dim_localizacao")
    logger.info("Dim_Localizacao criada com sucesso.")

    # Criar Dim_Veiculo
    logger.info("Criando dimensão Dim_Veiculo com fabricante...")

    dim_veiculo = veiculo.join(
        modelo,
        veiculo['veiculo_cd_modelo'] == modelo['modelo_cd_modelo'],
        'left'
    ).join(
        fabricante, 
        modelo['cd_fabricante'] == fabricante['fabricante_cd_fabricante'],
        'left'
    ).select(
        col('nr_placa').alias('id_veiculo'),
        col('modelo_nome').alias('modelo'),
        col('fabricante_cd_fabricante').alias('fabricante_cd'),
        col('nm_fabricante').alias('fabricante_nome'),
        col('tp_automovel').alias('tipo_automovel'),
        col('tp_combustivel').alias('tipo_combustivel')
    )

    dim_veiculo.write.mode("overwrite").parquet(f"{query_path}dim_veiculo")
    logger.info("Dim_Veiculo com fabricante criada com sucesso.")

    # Criar Dim_Agencia
    logger.info("Criando dimensão Dim_Agencia...")
    dim_agencia = agencia.join(
        dim_localizacao,
        agencia['agencia_cd_cidade'] == dim_localizacao['id_cidade'],
        'left'
    ).select(
        col('cd_agencia').alias('id_agencia'),
        col('nm_agencia').alias('nome_agencia'),
        dim_localizacao['id_cidade'],
        dim_localizacao['nome_cidade'],
        dim_localizacao['id_estado'],
        dim_localizacao['nome_estado'],
        dim_localizacao['sigla_estado']
    )
    dim_agencia.write.mode("overwrite").parquet(f"{query_path}dim_agencia")
    logger.info("Dim_Agencia criada com sucesso.")

    # Criar Fato_Desempenho_Veiculo
    logger.info("Criando tabela fato Fato_Desempenho_Veiculo...")
    fato_desempenho = pedido_locacao.join(
        veiculo,
        pedido_locacao['nr_pedido'] == veiculo['nr_placa'],
        'inner'
    ).groupBy(
        'nr_placa', 'cd_agencia'
    ).agg(
        count('*').alias('quantidade_locacoes'),
        sum('qt_dias').alias('total_dias_locados'),
        sum('vl_total').alias('total_receita'),
        avg('vl_total').alias('media_receita_por_locacao')
    ).join(
        dim_agencia,
        col('cd_agencia') == dim_agencia['id_agencia'],
        'left'
    ).select(
        col('nr_placa').alias('id_veiculo'),
        col('id_agencia'),
        col('quantidade_locacoes'),
        col('total_dias_locados'),
        col('total_receita'),
        col('media_receita_por_locacao'),
        dim_agencia['id_cidade'],
        dim_agencia['nome_cidade'],
        dim_agencia['id_estado'],
        dim_agencia['nome_estado'],
        dim_agencia['sigla_estado']
    )

    fato_desempenho.write.mode("overwrite").parquet(f"{query_path}fato_desempenho_veiculo")
    logger.info("Fato_Desempenho_Veiculo criada com sucesso e salva na camada de query.")

except Exception as e:
    logger.error(f"Erro ao criar a tabela Fato_Desempenho_Veiculo: {e}")

    



