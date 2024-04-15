from airflow import DAG
from airflow.utils import dates
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy

path_temp_csv = "/tmp/dataset.csv"
email_failed = "luizhad19@gmail.com"

dag = DAG(
    dag_id="etl-pipeline",
    description="Pipeline para ETL do ambiente OLTP ao OLAP",
    start_date=dates.days_ago(2),
    schedule_interval=None
)

def _extract():
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:mysql@172.17.0.2:3306/employees')

    dataset_df = pd.read_sql_query(r"""
                        SELECT   emp.emp_no
                        , emp.first_name
                        , emp.last_name
                        , sal.salary
                        , titles.title 
                        FROM employees emp 
                        INNER JOIN (SELECT emp_no, MAX(salary) as salary 
                                    FROM salaries GROUP BY emp_no) 
                        sal ON sal.emp_no = emp.emp_no 
                        INNER JOIN titles 
                        ON titles.emp_no = emp.emp_no
                        LIMIT 1000""",engine_mysql_oltp)
    
    dataset_df.to_csv(
        path_temp_csv, index=False
    )

def _transform():
    dataset_df = pd.read_csv(path_temp_csv)

    dataset_df["name"] = dataset_df["first_name"] + " " + dataset_df["last_name"]

    dataset_df.drop([
        "emp_no",
        "first_name",
        "last_name"
        ], 
        axis=1,
        inplace=True)
    
    dataset_df.to_csv( #persistindo e sobrescrevendo o dataset no csv temporario
        path_temp_csv,
        index=False
    )

def _load():
    #conexao com o postgresql olap
    engine_postgres_olap = sqlalchemy.create_engine('postgres+psycopg2://postgres:postgres@172.17.0.3:5432/employees')
    #lendo os dados do csv
    dataset_df = pd.read_csv(path_temp_csv)
    #carregando dados para a base de dados OLAP
    dataset_df.to_sql("employees_dataset", engine_postgres_olap, if_exists="replace", index=False)

#criando as dags do pipeline para o Airflow executar
    
extract_task = PythonOperator(
    task_id="extract_dataset",
    python_callable=_extract,
    email_on_failure=True,
    email=email_failed,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_dataset",
    email_on_failure=True,
    email=email_failed,
    python_callable=_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_dataset",
    email_on_failure=True,
    email=email_failed,
    python_callable=_load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean",
    email_on_failure=True,
    email=email_failed,
    bash_command="scripts/clean.sh",
    dag=dag
)

email_task = EmailOperator(
    task_id="notify",
    email_on_failure=True,
    email=email_failed,
    to='luizhad19@gmail.com',
    subject="Pipeline Finalizado!",
    html_content='<p> O Pipeline para atualização de dados entre os ambientes OLTP e OLAP foi finalizado com sucesso. <p>',
    dag=dag
)

extract_task >> transform_task >> load_task >> clean_task >> email_task