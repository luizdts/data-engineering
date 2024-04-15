from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import random

args = {
    'owner': 'aiflow',
    'start_date': days_ago(1),
    'description': 'DAG exemplo de retries'
}

def coleta_dados_web():
    n = random.random()
    print("Número aleatório {}".format(n))
    if n > 0.3:
        raise Exception("Erro!")
    print("Coleta ok!")

def processa_dados():
    print("Iniciando o processamento de dados")

with DAG(dag_id="retries_example",
        default_args=args,
        schedule_interval=None
        ) as dag:
    
        coleta_dados_task = PythonOperator(
             task_id="coleta_dados",
             python_callable=coleta_dados_web,
             retries=10,
             retry_delay=timedelta(seconds=30)
        )

        processa_dados_task = PythonOperator(
             task_id="processa_dados",
             python_callable=processa_dados
        )

coleta_dados_task >> processa_dados_task
