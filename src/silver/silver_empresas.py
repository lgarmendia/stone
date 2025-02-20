import os
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from src.utils import render

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("BronzeToSilver_Empresa") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Variaveis
BRONZE_FOLDER = render.get_yaml_value("bronze_location")  
SILVER_FOLDER = render.get_yaml_value("silver_location")  
DUCKDB_SILVER = render.get_yaml_value("duckdb_empresa_silver")  

# Esquema
EMPRESA_SCHEMA = StructType([
    StructField("cnpj", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificacao_responsavel", IntegerType(), True),
    StructField("capital_social", FloatType(), True),
    StructField("cod_porte", StringType(), True)
])

def get_latest_empresa_file(bronze_folder):
    """ Busca o arquivo mais recente de 'empresa' na Bronze """
    files = [f for f in os.listdir(bronze_folder) if f.startswith("empresas_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo de 'empresa' encontrado na camada Bronze")
    
    # Ordena os arquivos pelo timestamp no nome e pega o mais recente
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(bronze_folder, f)))
    return os.path.join(bronze_folder, latest_file)

def process_empresa_to_silver():
    """ Lê o CSV da Bronze, salva como Delta na Silver e permite leitura no DuckDB """
    try:
        empresa_file = get_latest_empresa_file(BRONZE_FOLDER)
        print(f"Processando arquivo: {empresa_file}")

        # Criar Dataframe
        df = spark.read.option("header", "true").option("delimiter", ";").schema(EMPRESA_SCHEMA).csv(empresa_file)

        silver_delta_path = os.path.join(SILVER_FOLDER, "empresa_delta")

        # Salvar como Delta
        df.write.format("delta").mode("overwrite").save(silver_delta_path)
        print(f"Dados salvos na Silver (Delta) em: {silver_delta_path}")

        # DuckDB
        duckdb_conn = duckdb.connect(DUCKDB_SILVER)

        duckdb_conn.execute(f"""
            CREATE OR REPLACE TABLE empresa AS 
            SELECT * FROM read_parquet('{silver_delta_path}/*.parquet')
        """)

        print(f"Dados salvos no DuckDB a partir dos Parquet do Delta: {DUCKDB_SILVER}")

    except Exception as e:
        print(f"Erro ao processar os dados da Bronze para Silver: {e}")

process_empresa_to_silver()
