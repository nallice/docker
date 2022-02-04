from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.wasb import WasbHook
import datetime

import requests

dt_i = datetime.date.today() - datetime.timedelta(days=2)
dt_f = datetime.date.today() - datetime.timedelta(days=1)

year = dt_f.year
month = dt_f.month
day = dt_f.day

bash_cmd_template = """
azcopy copy {{csv_file}} ""https://afipstlrsbrzdatalake001.dfs.core.windows.net/raw/Arlab/Incremental/{{ params.year }}/{{ params.month }}/{{ params.day }}?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-02-07T20:28:22Z&st=2021-12-07T12:28:22Z&spr=https&sig=PkAek8OWYN21IfDfpSoImlNMfp0W87knqtuSsnFiAyg%3D"
"""

def _request_url():
    url = f'http://ceacsul.com.br/w_relatorio_qlik/login.aspx?usu=NESS&senha=NESS@2019!&modelo=RELATORIO_NESS&dt_i={dt_i}&dt_f={dt_f}'

    response = requests.get(url)

    while response.status_code != 200:
        response = requests.get(url)

    url_content = response.content
    filename = f'relatorio_ness_{dt_i}_{dt_f}.csv'
    csv_file = open(filename, 'wb')
    csv_file.write(url_content)
    csv_file.close()

    wasb_hook = WasbHook(wasb_conn_id='wasb_default')
    wasb_hook.load_string(res.text, '{0}_{1}.csv'.format(state, date), bucket_name=bucket, replace=True)

    return 
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG("ingest_arlab_into_azure",
start_date=datetime(2022,1, 20),
schedule_interval='@daily',
default_args=default_args,
catchup=False # allows you to prevent from backfilling automatically the non triggered DAG Runs between the start date of your DAG and the current date
) as dag:

    request = PythonOperator(
        task_id='request_url',
        python_callable=_request_url
    )

    ingest = BashOperator(
        task_id='ingest_azure',
        bash_command=bash_cmd_template,
        params={
            'year': year,
            'month': month,
            'day': day
        }
    )