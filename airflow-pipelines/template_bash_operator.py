from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

with DAG(dag_id="template_bash_operator",
    description="A DAG de exemplo de utilização de templates com Bash Operator.",
    start_date=dt.datetime(2024, 4, 4),
    schedule_interval=None
    ) as dag:
    
    execute_script = BashOperator(
    task_id="execute_script",
    bash_command="/scripts/test.sh",
    env={'execution_date':'{{ ds }}'}
    )
   
    execute_script