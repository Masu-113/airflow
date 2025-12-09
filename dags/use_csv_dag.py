from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import pyodbc
from datetime import datetime
import os

def fetch_and_process_data():
    try:
        # crear el directorio si no existe
        csv_directory = '/home/msuarez/airflow/CSV'
        os.makedirs(csv_directory, exist_ok=True)

        # declaracion de la base de datos origen
        source_conn = BaseHook.get_connection('adventureworks_db')
        
        # Coneccion
        source_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_conn.host},{source_conn.port};DATABASE={source_conn.schema};UID={source_conn.login};PWD={source_conn.password}"
        source_connection = pyodbc.connect(source_connection_string)
        
        # consulta que toma los datos de columnas especificas
        #query = "SELECT SalesOrderID, RevisionNumber, ShipDate, Status FROM Sales.SalesOrderHeader;"
        query = "SELECT TOP 20 * FROM Sales.SalesOrderHeader;"
        df = pd.read_sql(query, source_connection)
        
        # Guardar todos los datos en un archivo CSV
        csv_file_path = os.path.join(csv_directory, 'sales_data.csv')
        df.to_csv(csv_file_path, index=False)  # Guarda el DataFrame como CSV sin el indice
        
        # Imprimir los datos originales (opcional)
        print("Datos originales guardados en CSV:")
        print(df)
        
        # Limpieza de datos solo para la insercion en la base de datos de destino
        df_cleaned = df.drop_duplicates(subset=['SalesOrderID'])  # Eliminar duplicados solo para la inserción
        df_cleaned.fillna(method='ffill', inplace=True)  # Rellenar valores nulos

        # Imprimir los datos limpios
        print("Datos limpios para la inserción:")
        print(df_cleaned)
        
        # declaracion de la base de datos destino
        destination_conn = BaseHook.get_connection('destination_db')
        
        # coneccion
        destination_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={destination_conn.host},{destination_conn.port};DATABASE={destination_conn.schema};UID={destination_conn.login};PWD={destination_conn.password}"
        destination_connection = pyodbc.connect(destination_connection_string)
        
        # Insertar datos en la base de datos de destino
        for index, row in df_cleaned.iterrows():
            try:
                insert_query = """INSERT INTO DatosPowerBI (SalesOrderID, RevisionNumber, ShipDate, Status) VALUES (?, ?, ?, ?)"""
                destination_connection.execute(insert_query, row['SalesOrderID'], row['RevisionNumber'], row['ShipDate'], row['Status'])
            except Exception as e:
                print(f"Error inserting row {row['SalesOrderID']}: {e}")

        destination_connection.commit() 
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # termina las conecciones
        if 'source_connection' in locals():
            source_connection.close()
        if 'destination_connection' in locals():
            destination_connection.close()

# aqui se define el DAG
with DAG(
    'fetch_and_process_data_csv',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_and_process_data,
    )

    fetch_data_task
