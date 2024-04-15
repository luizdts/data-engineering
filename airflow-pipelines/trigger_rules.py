from airflow import DAG
from airflow.utils import dates
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="trigger_rules",
    description="DAG para conceito de trigger rules",
    start_date=dates.days_ago(14),
    schedule_interval=None,
)

def check_acurracy(ti):
    acurracy_value = int(ti.xcom_pull(key='model_acurracy'))
    if acurracy_value >= 90:
        return "deploy_task"
    else:
        return "retrain_task"

get_acurracy_op = BashOperator(
    task_id='get_acurracy_op',
    bash_command='echo "{{ ti.xcom_push(key="model_acurracy", value=75) }}"',
    dag=dag
)

check_acurracy_op = BranchPythonOperator(
    task_id='check_acurracy_task',
    python_callable=check_acurracy,
    dag=dag
)

deploy_op = DummyOperator(task_id="deploy_task", dag=dag)
retrain_op = DummyOperator(task_id="retrain_task", dag=dag)
notify_op = DummyOperator(task_id="notify_task", trigger_rule=TriggerRule.NONE_FAILED, dag=dag)

get_acurracy_op >> check_acurracy_op >> [deploy_op, retrain_op] >> notify_op