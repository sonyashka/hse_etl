import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


def extract(**kwargs):
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    client = mongo_hook.get_conn()
    collection = client['source']['user_sessions']
    kwargs['ti'].xcom_push(key='mongo_data', value=collection.find({}, {'_id': 0}))

def load(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    engine = pg_hook.get_sqlalchemy_engine()
    data_to_load = kwargs['ti'].xcom_pull(task_ids='extract', key='mongo_data')

    df = pd.DataFrame(data_to_load)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_date'])
    df.to_sql('user_sessions', con=engine, if_exists='replace')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 14),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    'load_user_sessions',
    catchup=False,
    default_args=default_args
) as dag:

    te = PythonOperator(task_id='te', python_callable=extract)
    tl = PythonOperator(task_id='tl', python_callable=load)

    te >> tl