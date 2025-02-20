import os
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from src.utils import render

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Variaveis
SILVER_FOLDER = render.get_yaml_value("silver_location")
GOLD_FOLDER = render.get_yaml_value("gold_location")
DUCKDB_GOLD = render.get_yaml_value("duckdb_gold")

# Esquema final
GOLD_SCHEMA = StructType([
    StructField("cnpj", StringType(), True),
    StructField("qtde_socios", IntegerType(), True),
    StructField("flag_socio_estrangeiro", BooleanType(), True),
    StructField("doc_alvo", BooleanType(), True)
])

def process_silver_to_gold():
    """Lê os dados da Silver, realiza transformações e salva na camada Gold."""
    try:
        # Caminho para as tabelas Delta na Silver
        socio_silver_path = os.path.join(SILVER_FOLDER, "socio_delta")
        empresa_silver_path = os.path.join(SILVER_FOLDER, "empresa_delta")

        # Lê os dados da Silver
        df_socio = spark.read.format("delta").load(socio_silver_path)
        df_empresa = spark.read.format("delta").load(empresa_silver_path)

        # Criar as transformações para a Gold
        df_socio_agg = df_socio.groupBy("cnpj").agg(
            count("*").alias("qtde_socios"),
            max(when(col("codigo_qualificacao_socio") == "foreign", lit(True)).otherwise(lit(False))).alias("flag_socio_estrangeiro")
        )

        # Junta com os dados da empresa para calcular a flag doc_alvo
        df_gold = df_socio_agg.join(df_empresa, "cnpj", "left").select(
            df_socio_agg["cnpj"],
            df_socio_agg["qtde_socios"],
            df_socio_agg["flag_socio_estrangeiro"],
            when((col("cod_porte") == "03") & (col("qtde_socios") > 1), lit(True)).otherwise(lit(False)).alias("doc_alvo")
        )

        # Salvar a Gold
        gold_delta_path = os.path.join(GOLD_FOLDER, "gold_empresas")

        df_gold.write.format("delta").mode("overwrite").save(gold_delta_path)

        print(f"Dados salvos na Gold em: {gold_delta_path}")

        # DuckDB lê os arquivos Parquet dentro da tabela Delta
        duckdb_conn = duckdb.connect(DUCKDB_GOLD)

        duckdb_conn.execute(f"""
            CREATE OR REPLACE TABLE gold_empresas AS 
            SELECT * FROM read_parquet('{gold_delta_path}/*.parquet')
        """)

        print(f"Dados salvos no DuckDB: {DUCKDB_GOLD}")

    except Exception as e:
        print(f"Erro ao processar os dados da Silver para Gold: {e}")

# 🔹 Executa o pipeline
process_silver_to_gold()
