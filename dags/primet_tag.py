from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import psycopg2
from datetime import datetime

def fetch_data_from_db():
    # Obtener la conexión
    conn = BaseHook.get_connection('adventureworks_db')
    
    # Conectar a la base de datos
    connection = psycopg2.connect(
        host=conn.host,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    
    # Ejecutar una consulta
    query = "SELECT TOP 10 * FROM Sales.SalesOrderHeader;"
    df = pd.read_sql(query, connection)
    
    # Imprimir los datos (o hacer algo con ellos)
    print(df)
    
    # Cerrar la conexión
    connection.close()

# Definición del DAG
with DAG(
    'fetch_adventureworks_data',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_db,
    )

    fetch_data_task
