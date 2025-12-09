from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import pyodbc  # Cambia de psycopg2 a pyodbc
from datetime import datetime

def fetch_data_from_db():
    try:
        # Obtener la conexión
        conn = BaseHook.get_connection('adventureworks_db')
        
        # Conectar a la base de datos usando pyodbc
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={conn.host},{conn.port};DATABASE={conn.schema};UID={conn.login};PWD={conn.password}"
        connection = pyodbc.connect(connection_string)
        
        # Ejecutar una consulta
        query = "SELECT TOP 10 SalesOrderID,RevisionNumber, ShipDate, Status FROM Sales.SalesOrderHeader;"
        df = pd.read_sql(query, connection)
        
        # Imprimir los datos (o hacer algo con ellos)
        print(df)

        


    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # Cerrar la conexión
        if 'connection' in locals():
            connection.close()




# Definición del DAG
with DAG(
    'fetch_adventureworks_data2',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_db,
    )

    fetch_data_task
