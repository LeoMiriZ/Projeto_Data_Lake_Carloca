from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, lower, when
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

spark = SparkSession.builder.appName("OrganizationLayer").getOrCreate()

# Caminhos das camadas
ingestion_path = "/mnt/datalake/ingestion/"
organization_path = "/mnt/datalake/organization/"

# Tabelas para processamento
tables = [
    'loc_pedido_locacao', 'loc_cliente', 'loc_veiculo', 'loc_agencia', 
    'loc_item_locacao', 'loc_funcionario', 'loc_cidade', 'loc_estado',
    'loc_tp_cliente', 'loc_tp_automovel', 'loc_tp_combustivel', 
    'loc_fabricante', 'loc_modelo'
]

# Processamento das tabelas
for t in tables:
    try:
        # Carregar os dados da camada de ingestão
        logger.info(f"Carregando dados da tabela {t} da camada de ingestão.")
        df = spark.read.parquet(f"{ingestion_path}{t}")

        # Remoção de duplicatas
        df_limpo = df.dropDuplicates()

        # Normalização de strings
        string_columns = [field.name for field in df_limpo.schema.fields if str(field.dataType) == 'StringType']
        for col_name in string_columns:
            df_limpo = df_limpo.withColumn(col_name, trim(lower(col(col_name))))

        # Conversão de datas
        date_columns = [c for c in df_limpo.columns if 'data' in c.lower()]
        for date_col in date_columns:
            logger.info(f"Convertendo coluna {date_col} para o tipo Date na tabela {t}.")
            df_limpo = df_limpo.withColumn(date_col, to_date(col(date_col), 'yyyy-MM-dd'))

        # Padronização de status
        if 'status' in df_limpo.columns:
            logger.info(f"Padronizando coluna 'status' na tabela {t}.")
            df_limpo = df_limpo.withColumn('status', when(col('status') == 'A', 'Ativo')
                                            .when(col('status') == 'I', 'Inativo')
                                            .otherwise('Desconhecido'))

        # Colunas calculadas
        if t == 'loc_pedido_locacao':
            logger.info(f"Adicionando colunas calculadas na tabela {t}.")
            df_limpo = df_limpo.withColumn('qt_dias', 
                                           (col('dt_entrega').cast('long') - col('dt_locacao').cast('long')) / 86400)
            df_limpo = df_limpo.withColumn('valor_total_calculado', col('qt_dias') * col('vl_total'))

        # Relacionamentos entre tabelas
        if t == 'loc_veiculo':
            logger.info(f"Adicionando relacionamento entre veículos e modelos.")
            df_modelo = spark.read.parquet(f"{ingestion_path}loc_modelo")
            df_limpo = df_limpo.join(df_modelo, df_limpo['cd_modelo'] == df_modelo['cd_modelo'], 'left') \
                               .drop(df_modelo['cd_modelo'])

        # Contagem de registros
        record_count = df_limpo.count()
        logger.info(f"Tabela {t} processada com {record_count} registros após limpeza e transformação.")

        logger.info(f"Salvando tabela {t} na camada de organização.")
        df_limpo.write.mode("overwrite").parquet(f"{organization_path}{t}")
        logger.info(f"Tabela {t} salva com sucesso na camada de organização.")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {t}: {e}")




