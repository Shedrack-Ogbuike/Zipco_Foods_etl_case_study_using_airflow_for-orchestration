# Importing necessary libraries
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

def run_loading():
    # Load the cleaned data files
    data = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/data/cleaned_data.csv')
    products = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/data/customers.csv')
    customers = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/data/products.csv')
    staff = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/data/staff.csv')
    transactions = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/data/transactions.csv')
    # Load environment variables
    load_dotenv()
    
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Upload files to Azure Blob Storage
    files = [ (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
          (products, 'cleaned/products.csv'),
          (customers, 'cleaned/customers.csv'),
          (staff, 'cleaned/staff.csv'),
          (transactions, 'cleaned/transactions.csv')
          ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f"Uploaded {blob_name} to Azure Blob Storage.")
   
    
    