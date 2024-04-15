from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="scheduler-daily",
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 30)
)

print_date_task = BashOperator(
    task_id="print_date",
    bash_command=(
        "echo Iniciando tarefa: &&"
        "date"
    ),
    dag=dag
)

processing_task = BashOperator(
    task_id="processing_data",
    bash_command=(
        "echo Processando dados... &&"
        "sleep 15"
    ),
    dag=dag
)

print_date_task >> processing_task