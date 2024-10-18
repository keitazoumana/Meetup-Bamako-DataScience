# @Author: Zoumana Keita

from botocore.exceptions import ClientError
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import os
from typing import Tuple
from config import config
import boto3
from io import BytesIO

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    parts = s3_path.replace("s3://", "").split("/")
    return parts[0], "/".join(parts[1:])

def text_extractor(input_s3_path: str, output_s3_path: str) -> Tuple[dict, str]:
    # Parse S3 paths
    input_bucket, input_key = parse_s3_path(input_s3_path)
    output_bucket, output_prefix = parse_s3_path(output_s3_path)

    # Initialize clients
    s3_client = boto3.client('s3')
    document_analysis_client = DocumentAnalysisClient(
        endpoint=config.OCR_ENDPOINT, 
        credential=AzureKeyCredential(config.OCR_KEY)
    )

    # Download file from S3 to memory
    input_file = BytesIO()
    s3_client.download_fileobj(input_bucket, input_key, input_file)
    input_file.seek(0)

    # Analyze document
    poller = document_analysis_client.begin_analyze_document(
        model_id=config.OCR_MODEL_ID, 
        document=input_file,
        polling_interval=5,
        logging_enable=False
    )
    ocr_results = poller.result()

    # Extract results
    content = ocr_results.content
    result_dict = ocr_results.to_dict()

    # Prepare output file name and path
    input_file_name = os.path.basename(input_key)
    output_file_name = os.path.splitext(input_file_name)[0] + ".txt"
    
    # Construct the final output key
    output_key = f"{output_prefix}/{output_file_name}"

    # Upload extracted text to S3
    s3_client.put_object(Body=content, Bucket=output_bucket, Key=output_key)

    print(f"Text file saved to s3://{output_bucket}/{output_key}")
    return result_dict, content

def list_s3_files(bucket: str, prefix: str) -> list:
    """List files in an S3 bucket with a given prefix."""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [item['Key'] for item in response.get('Contents', []) if not item['Key'].endswith('/')]
    except ClientError as e:
        print(f"An error occurred: {e}")
        return []