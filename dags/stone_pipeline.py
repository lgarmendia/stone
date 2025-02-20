"""Master pipeline."""

import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# Definir funções para executar os scripts locais.
def run_local_bronze_python_script():
    """Execute o script Python da camada Bronze que processa os dados brutos.

        O script está localizado em /opt/airflow/src/bronze/bronze_stone.py e é executado usando o interpretador Python.

    """
    script_path_bronze = "/opt/airflow/src/bronze/bronze_stone.py"
    subprocess.run(["python", script_path_bronze], check=True)


def run_local_silver_socios_python_script():
    """Execute o script Python da camada Silver que processa os dados limpos.
        O script está localizado em /opt/airflow/src/silver/silver_socio.py e é executado usando o interpretador Python.

    """
    script_socios_path_silver = "/opt/airflow/src/silver/silver_socio.py"
    subprocess.run(["python", script_socios_path_silver], check=True)

def run_local_silver_empresas_python_script():
    """Execute o script Python da camada Silver que processa os dados limpos.
        O script está localizado em /opt/airflow/src/silver/silver_empresas.py e é executado usando o interpretador Python.

    """
    script_empresas_path_silver = "/opt/airflow/src/silver/silver_empresas.py"
    subprocess.run(["python", script_empresas_path_silver], check=True)
    


def run_local_gold_python_script():
    """Execute o script Python da camada Gold que processa o conjunto de dados final.

        O script está localizado em /opt/airflow/src/gold/gold_stone.py e é executado usando o interpretador Python.

    """
    script_gold_path = "/opt/airflow/src/gold/gold_stone.py"
    subprocess.run(["python", script_gold_path], check=True)


# Argumentos padrão para a DAG.
default_args = {
    "owner": "Test user",
    "email": ["test@test.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definir a DAG
with DAG(
    "stone_pipeline",
    default_args=default_args,
    description="DAG to execute the gold, silver, and bronze stone scripts.",
    start_date=datetime(2024, 9, 12),
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
) as dag:
    # Iniciar a task
    start = PythonOperator(task_id="start", python_callable=lambda: print("Jobs started"))

    # Bronze task
    bronze_stone = PythonOperator(
        task_id="bronze_stone", python_callable=run_local_bronze_python_script
    )

    # Silver empresas task
    silver_empresas_stone = PythonOperator(
        task_id="silver_empresas_stone", python_callable=run_local_silver_empresas_python_script
    )

    # Silver socios task
    silver_socios_stone = PythonOperator(
        task_id="silver_socios_stone", python_callable=run_local_silver_socios_python_script
    )

    # Gold task
    gold_stone = PythonOperator(
        task_id="gold_stone", python_callable=run_local_gold_python_script
    )

    # End task
    end = PythonOperator(
        task_id="end", python_callable=lambda: print("Jobs completed successfully")
    )

    # Definir as dependências das tarefas
    start >> bronze_stone >> silver_empresas_stone >> silver_socios_stone >> gold_stone >> end
