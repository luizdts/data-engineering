import datetime as dt
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="pipeline_data_finance",
    description="A DAG de teste para coletar dados do mercado financeiro por datas dinamicas.",
    schedule_interval=None,
    start_date=dt.datetime(2023, 4, 1),
    end_date=dt.datetime(2024, 4, 7)
)

def get_data_stocks(**context):

    start_date = context["templates_dict"]["start_date"]
    end_date = context["templates_dict"]["end_date"]
    ativo = "AAPL"
    print("Start date: {}".format(start_date))

    datafile = "/tmp/dataset-{}.csv".format(start_date)
    api_key = "75FtNC8upcXjrjy7pIuUadI3U8sOr3zV"
    
    url = 'https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/{}/{}?apiKey={}'.format(ativo,start_date,end_date,api_key)
    
    r = requests.get(url)
    data = r.json()

    if data["status"]=="OK":  
        #transformando o resultado em um Dataframe
        df = pd.DataFrame(data["results"])
        df["date"] = start_date

        #exportando os dados para o disco.
        df.to_csv(datafile,index=False)

get_data_stocks_task = PythonOperator(
    task_id="get_data_stocks", 
    python_callable=get_data_stocks,
    templates_dict={
        "start_date": "{{ ds }}",
        "end_date": "{{ ds }}",
    }, 
    dag=dag
)

get_data_stocks_task
