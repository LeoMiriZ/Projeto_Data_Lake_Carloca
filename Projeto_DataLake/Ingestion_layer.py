import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Parâmetros de conexão
host = "bdtrab.mysql.database.azure.com"
port = "3306"
database = "carloca"
user = "leonardo"
password = "jkxK48%nPzUvLW#"
use_ssl = True
output_path = "/mnt/datalake/ingestion"

# Lista de tabelas
tables = [
    'loc_age_bco', 'loc_agencia', 'loc_banco', 'loc_cargo', 'loc_cidade',
    'loc_cliente', 'loc_cor', 'loc_depto', 'loc_estado', 'loc_fabricante',
    'loc_fone_cliente', 'loc_funcionario', 'loc_grupo', 'loc_item_locacao',
    'loc_modelo', 'loc_operadora', 'loc_pedido_locacao', 'loc_proprietario',
    'loc_tp_automovel', 'loc_tp_cliente', 'loc_tp_combustivel', 'loc_veiculo'
]

spark = SparkSession.builder.appName("IngestionLayer").getOrCreate()

# Processamento
for t in tables:
    try:
        df = spark.read.format('mysql') \
            .option("useSSL", use_ssl) \
            .option("host", host) \
            .option("port", port) \
            .option("database", database) \
            .option("dbtable", t) \
            .option("user", user) \
            .option("password", password) \
            .load()

        logger.info(f"Esquema da tabela {t}:")
        df.printSchema()

        output_table_path = f"{output_path}/{t}"
        df.write.mode("overwrite").parquet(output_table_path)
        logger.info(f"Tabela {t} carregada e salva em {output_table_path}.")

    except Exception as e:
        logger.error(f"Erro ao processar tabela {t}: {e}")




