from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

spark = SparkSession.builder.appName("QueryLayer").getOrCreate()

# Caminhos das camadas
organization_path = "/mnt/datalake/organization/"
query_path = "/mnt/datalake/query/"

# Lista de tabelas
tables = [
    'loc_age_bco', 'loc_agencia', 'loc_banco', 'loc_cargo', 'loc_cidade',
    'loc_cliente', 'loc_cor', 'loc_depto', 'loc_estado', 'loc_fabricante',
    'loc_fone_cliente', 'loc_funcionario', 'loc_grupo', 'loc_item_locacao',
    'loc_modelo', 'loc_operadora', 'loc_pedido_locacao', 'loc_proprietario',
    'loc_tp_automovel', 'loc_tp_cliente', 'loc_tp_combustivel', 'loc_veiculo'
]

# Processamento
for t in tables:
    try:
        # Carregar os dados da camada de organização
        logger.info(f"Carregando dados da tabela {t} da camada de organização.")
        df = spark.read.parquet(f"{organization_path}{t}")

        # Contar o número de registros por tipo de cliente
        if 'loc_tp_cliente' in df.columns:
            logger.info(f"Realizando agregação para a tabela {t}.")
            df_agregado = df.groupBy('loc_tp_cliente').agg(
                F.count('*').alias('total_clientes')
            )
            df_agregado.write.mode("overwrite").parquet(f"{query_path}{t}_aggregated")
            logger.info(f"Dados agregados para {t} salvos na camada de query.")

        # Unir dados de clientes com locações
        if t == 'loc_cliente':
            logger.info(f"Realizando join para a tabela {t} com 'loc_item_locacao'.")
            df_locacoes = spark.read.parquet(f"{organization_path}loc_item_locacao")
            df_joined = df.join(df_locacoes, df['cd_cliente'] == df_locacoes['nr_pedido'], 'inner')
            df_joined = df_joined.select(
                df['cd_cliente'], df['nm_cliente'], df['cd_tp_cliente'], 
                df_locacoes['vl_diaria'], df_locacoes['nr_pedido']
            )
            df_joined.write.mode("overwrite").parquet(f"{query_path}{t}_joined")
            logger.info(f"Tabela {t} unida e salva na camada de query.")

        # Aplicar filtros
        if t == 'loc_pedido_locacao':
            logger.info(f"Aplicando filtro na tabela {t}.")
            df_filtrado = df.filter(df['status'] == 'A')  # Filtro de status ativo
            df_filtrado.write.mode("overwrite").parquet(f"{query_path}{t}_filtered")
            logger.info(f"Dados filtrados da tabela {t} salvos na camada de query.")

        # Criar uma coluna calculada
        if t == 'loc_pedido_locacao':
            logger.info(f"Adicionando colunas calculadas na tabela {t}.")
            df_calculado = df.withColumn('total_calculado', df['qt_dias'] * df['vl_total'])
            df_calculado.write.mode("overwrite").parquet(f"{query_path}{t}_calculated")
            logger.info(f"Tabela {t} com colunas calculadas salva na camada de query.")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {t}: {e}")

        


