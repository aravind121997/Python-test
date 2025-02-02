import os
from google.cloud import bigquery
from azure.storage.filedataset import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
import pandas as pd
from datetime import datetime
import logging

def transfer_data(req):
    try:
        # Configure logging
        logging.info("Starting data transfer process")
        
        # GCP credentials - ensure service account JSON is set in environment
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/service-account.json"
        bq_client = bigquery.Client()
        
        # Azure credentials
        account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
        credential = DefaultAzureCredential()
        datalake_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential
        )
        
        # BigQuery settings
        project_id = os.environ["GCP_PROJECT_ID"]
        dataset_id = os.environ["BQ_DATASET"]
        table_id = os.environ["BQ_TABLE"]
        
        # Azure Data Lake settings
        container_name = os.environ["ADLS_CONTAINER"]
        directory_name = os.environ["ADLS_DIRECTORY"]
        
        # Create filesystem client
        filesystem_client = datalake_client.get_file_system_client(file_system=container_name)
        directory_client = filesystem_client.get_directory_client(directory_name)
        
        # Query data in chunks
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        """
        
        # Stream data in chunks to handle large datasets
        chunk_size = 100000  # Adjust based on your memory constraints
        query_job = bq_client.query(query)
        
        # Generate timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chunk_number = 0
        
        for rows in query_job.result(page_size=chunk_size):
            chunk_number += 1
            
            # Convert chunk to DataFrame
            df_chunk = pd.DataFrame([dict(row.items()) for row in rows])
            
            if not df_chunk.empty:
                # Convert to parquet format
                parquet_data = df_chunk.to_parquet()
                
                # Create file name with timestamp and chunk number
                file_name = f"data_{timestamp}_chunk_{chunk_number}.parquet"
                file_client = directory_client.create_file(file_name)
                
                # Upload data
                file_client.append_data(data=parquet_data, offset=0)
                file_client.flush_data(len(parquet_data))
                
                logging.info(f"Uploaded chunk {chunk_number} to {file_name}")
            
        return {
            "status": "success",
            "message": f"Transferred {chunk_number} chunks successfully",
            "timestamp": timestamp
        }
        
    except Exception as e:
        logging.error(f"Error in transfer process: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Function app trigger (HTTP trigger example)
def main(req):
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        result = transfer_data(req)
        return {
            "status": 200,
            "body": result,
            "headers": {
                "Content-Type": "application/json"
            }
        }
    except Exception as e:
        return {
            "status": 500,
            "body": {"error": str(e)},
            "headers": {
                "Content-Type": "application/json"
            }
        }