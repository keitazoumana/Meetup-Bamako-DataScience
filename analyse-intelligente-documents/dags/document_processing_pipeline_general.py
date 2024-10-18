# @Author: Zoumana Keita

import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import boto3
import logging

from text_extractor_general import text_extractor
from metadata_extractor_general import metadata_extractor

business_requirements_path = "./config/business_request.yaml"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 12),
    'email_on_failure': True,
    'email': ['zoumana930@hotmail.fr'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_yaml_config():
    yaml_file_path = os.path.join(os.path.dirname(__file__), 'config', 'business_request.yaml')
    
    try:
        with open(yaml_file_path, 'r') as file:
            config = yaml.safe_load(file)
            if config is None:
                logging.error("YAML file is empty or invalid")
                return None
            return config
    except FileNotFoundError:
        logging.error(f"YAML configuration file not found at {yaml_file_path}")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration: {e}")
        return None

# Load configuration from YAML file
config = read_yaml_config()

# Extract S3 paths from config
input_bucket, input_prefix = config['data_source']['input']['path'].replace("s3://", "").split("/", 1)
output_bucket = config['data_source']['output']['metadata_path'].replace("s3://", "").split("/", 1)[0]
team_project = input_prefix.split('/')[0]  # Assuming team project is the first folder in the input prefix

def process_document(input_s3_path: str, output_s3_path: str, file_name: str):
    logging.info(f"Processing document: {file_name}")

    # Step 1: Extract text from the document
    try:
        ocr_output_path = f"s3://{output_bucket}/{team_project}/output/ocr-raw-text"
        result_dict, content = text_extractor(input_s3_path, ocr_output_path)
        logging.info(f"Text extracted successfully from {file_name}")
    except Exception as e:
        logging.error(f"Error extracting text from {file_name}: {str(e)}")
        raise

    # Step 2: Extract metadata from the extracted text
    try:
        metadata_output_path = f"s3://{output_bucket}/{team_project}/output/metadata"
        metadata = metadata_extractor(content, metadata_output_path, file_name)
        if metadata:
            logging.info(f"Metadata extracted successfully from {file_name}")
        else:
            logging.warning(f"No metadata extracted from {file_name}")
    except Exception as e:
        logging.error(f"Error extracting metadata from {file_name}: {str(e)}")
        raise


def list_and_process_documents():
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
    
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        file_name = file_key.split('/')[-1]
        
        if file_name.lower().endswith(('.pdf', '.docx', '.txt')):
            input_s3_path = f"s3://{input_bucket}/{file_key}"
            output_s3_path = f"s3://{output_bucket}/{team_project}/output"
            
            process_document(input_s3_path, output_s3_path, file_name)

# Define the DAG
dag = DAG(
    'analyse-intelligente-documents',
    default_args=default_args,
    description='Un DAG pour le traitement de documents avec extraction de texte, extraction de métadonnées, et création de PDF consultables',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

process_documents_task = PythonOperator(
    task_id='process_documents',
    python_callable=list_and_process_documents,
    dag=dag
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Set up the task dependencies
start_task >> process_documents_task >> end_task