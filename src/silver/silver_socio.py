import os
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils import render

# Configuração do Spark
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Variaveis
BRONZE_FOLDER = render.get_yaml_value("bronze_location")  
SILVER_FOLDER = render.get_yaml_value("silver_location")  
DUCKDB_SILVER = render.get_yaml_value("duckdb_socio_silver")  

# Esquema fixo do CSV
SOCIO_SCHEMA = StructType([
    StructField("cnpj", StringType(), True),
    StructField("tipo_socio", IntegerType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("documento_socio", StringType(), True),
    StructField("codigo_qualificacao_socio", StringType(), True)
])

def get_latest_socio_file(bronze_folder):
    files = [f for f in os.listdir(bronze_folder) if f.startswith("socio_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo de 'socio' encontrado na camada Bronze")
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(bronze_folder, f)))
    return os.path.join(bronze_folder, latest_file)

def process_socio_to_silver():
    try:
        socio_file = get_latest_socio_file(BRONZE_FOLDER)
        print(f"Processando arquivo: {socio_file}")

        df = spark.read.option("header", "true").option("delimiter", ";").schema(SOCIO_SCHEMA).csv(socio_file)

        # Salvar como Delta
        silver_delta_path = os.path.join(SILVER_FOLDER, "socio_delta")
        df.write.format("delta").mode("overwrite").save(silver_delta_path)
        print(f"Dados Delta salvos em: {silver_delta_path}")

        # DuckDB
        duckdb_dir = os.path.dirname(DUCKDB_SILVER)
        if not os.path.exists(duckdb_dir):
            os.makedirs(duckdb_dir, exist_ok=True)

        duckdb_conn = duckdb.connect(DUCKDB_SILVER)
        duckdb_conn.execute(f"""
            CREATE OR REPLACE TABLE socio AS 
            SELECT * FROM read_parquet('{silver_delta_path}/*.parquet')
        """)
        print(f"Dados salvos no DuckDB: {DUCKDB_SILVER}")

    except Exception as e:
        print(f"Erro: {e}")

process_socio_to_silver()