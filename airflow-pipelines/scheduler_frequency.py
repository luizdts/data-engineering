import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="scheduler-frequency",
    description="DAG frequency-based no Airflow",
    schedule_interval=dt.timedelta(minutes=5),
    start_date=dt.datetime(2024, 3, 31)
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