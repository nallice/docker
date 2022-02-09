from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import datetime
import pandas as pd

import requests

def _request_url():

    dt_i = datetime.date.today() - datetime.timedelta(days=2)
    dt_f = datetime.date.today() - datetime.timedelta(days=1)

    day_init = str(dt_i).split('-')[2][:2]
    month_init = str(dt_i).split('-')[1]
    year_init = str(dt_i).split('-')[0]
    day_fin = str(dt_f).split('-')[2][:2]
    month_fin = str(dt_f).split('-')[1]
    year_fin = str(dt_f).split('-')[0]

    url_cad = f'http://ceacsul.com.br/w_relatorio_qlik/login.aspx?usu=NESS&senha=NESS@2019!&modelo=RELATORIO_NESS&dt_i={dt_i}&dt_f={dt_f}'
    
    response_cad = requests.get(url_cad)

    while response_cad.status_code != 200:
        response_cad = requests.get(url_cad)

    url_content_cad = response_cad.content
    filename_cad = f'RELATORIO_NESS_CADASTRADOS{day_init}-{month_init}-{year_init}_{day_fin}-{month_fin}-{year_fin}.csv'
    csv_file = open(filename_cad, 'wb')
    csv_file.write(url_content_cad)
    csv_file.close()

    wasb_hook = WasbHook(wasb_conn_id='wasb-default')
    wasb_hook.load_file(filename_cad, container_name='raw/', blob_name=f'Arlab/{year_fin}/{month_fin}/{day_fin}/{filename_cad}')

    url_lib = f'http://ceacsul.com.br/w_relatorio_qlik/login.aspx?usu=NESS&senha=NESS@2019!&modelo=QLIK_ASSINADO&dt_i={dt_i}&dt_f={dt_f}'
    
    response_lib = requests.get(url_lib)

    while response_lib.status_code != 200:
        response_lib = requests.get(url_lib)

    url_content_lib = response_lib.content
    filename_lib = f'RELATORIO_NESS_LIBERADOS{day_init}-{month_init}-{year_init}_{day_fin}-{month_fin}-{year_fin}.csv'
    csv_file = open(filename_lib, 'wb')
    csv_file.write(url_content_lib)
    csv_file.close()

    wasb_hook.load_file(filename_lib, container_name='raw/', blob_name=f'Arlab/{year_fin}/{month_fin}/{day_fin}/{filename_lib}')

    return 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG("ingest_arlab_into_azure",
start_date=datetime.datetime(2022,1, 30),
schedule_interval='@daily',
default_args=default_args,
catchup=False # allows you to prevent from backfilling automatically the non triggered DAG Runs between the start date of your DAG and the current date
) as dag:

    request = PythonOperator(
        task_id='request_url',
        python_callable=_request_url
    )
