from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import pyodbc
from datetime import datetime

def fetch_and_process_data():
    try:
        # Obtener la conexión a la base de datos de origen
        source_conn = BaseHook.get_connection('adventureworks_db')
        
        # Conectar a la base de datos de origen
        source_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_conn.host},{source_conn.port};DATABASE={source_conn.schema};UID={source_conn.login};PWD={source_conn.password}"
        source_connection = pyodbc.connect(source_connection_string)
        
        # Ejecutar una consulta
        query = "SELECT TOP 10 SalesOrderID, RevisionNumber, ShipDate, Status FROM Sales.SalesOrderHeader;"
        df = pd.read_sql(query, source_connection)
        
        # Limpieza de datos (ejemplo)
        df.drop_duplicates(inplace=True)  # Eliminar duplicados
        df.fillna(method='ffill', inplace=True)  # Rellenar valores nulos

        # Imprimir los datos limpios (opcional)
        print("Datos limpios:")
        print(df)
        
        # Obtener la conexión a la base de datos de destino
        destination_conn = BaseHook.get_connection('destination_db')  # Cambia 'destination_db' por el ID de tu conexión de destino
        
        # Conectar a la base de datos de destino
        destination_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={destination_conn.host},{destination_conn.port};DATABASE={destination_conn.schema};UID={destination_conn.login};PWD={destination_conn.password}"
        destination_connection = pyodbc.connect(destination_connection_string)
        
        # Insertar datos en la base de datos de destino
        for index, row in df.iterrows():
            insert_query = """INSERT INTO DatosPowerBI (SalesOrderID, RevisionNumber, ShipDate, Status) VALUES (?, ?, ?, ?)"""
            destination_connection.execute(insert_query, row['SalesOrderID'], row['RevisionNumber'], row['ShipDate'], row['Status'])
        destination_connection.commit() 
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # Cerrar las conexiones
        if 'source_connection' in locals():
            source_connection.close()
        if 'destination_connection' in locals():
            destination_connection.close()



# Definición del DAG
with DAG(
    'fetch_and_process_data3',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_and_process_data,
    )

    fetch_data_task
