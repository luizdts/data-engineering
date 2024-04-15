from airflow import DAG
from airflow.utils import dates
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="hello_world",
    description="Primeira DAG do Airflow",
    start_date=dates.days_ago(14),
    schedule_interval="@daily"
)

task_echo_message = BashOperator(
    task_id="echo_message",
    bash_command="echo Hello World from Airflow!",
    dag=dag
)

task_echo_message