# @Author: Zoumana Keita

import json
import os
import boto3
from io import StringIO
import csv
from tenacity import retry, stop_after_attempt, wait_random_exponential
from openai import AzureOpenAI
from config import config
from typing import Dict, Optional

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def completion_with_backoff(**kwargs):
    extraction_client = AzureOpenAI(
            api_key=config.GPT_KEY,
            api_version=config.OPENAI_API_VERSION,
            azure_endpoint=config.OPENAI_END_POINT,
        )
    
    return extraction_client.chat.completions.create(**kwargs)
        
def metadata_extractor(content: str, output_s3_path: str, file_name: str) -> Optional[Dict]:
    try:
        response = completion_with_backoff(
            model=config.GPT_DEPLOYMENT_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "Vous êtes un expert en analyse d'articles de recherche scientifique",
                },
                {
                    "role": "user",
                    "content": open(config.GPT_PROMPT_PATH, "r")
                    .read()
                    .replace("{document}", content),
                },
            ],
            temperature=config.GPT_TEMPERATURE,
            max_tokens=config.GPT_MAX_TOKEN,
        )
        result_str = response.choices[0].message.content
        result_str = result_str[result_str.find("```json") + 7 :]
        result_str = result_str[: result_str.find("```")]
        result_dict = json.loads(result_str)

        # Prepare S3 path for metadata
        s3_client = boto3.client('s3')
        output_bucket, output_prefix = output_s3_path.replace("s3://", "").split("/", 1)
        
        # Use the original file name but change the extension to .json
        output_file_name = os.path.splitext(os.path.basename(file_name))[0] + ".json"
        output_key = f"{output_prefix}/{output_file_name}"

        # Upload JSON to S3
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=json.dumps(result_dict, ensure_ascii=False, indent=2)
        )

        print(f"Metadata JSON saved to s3://{output_bucket}/{output_key}")
        return result_dict
    except Exception as e:
        print(f"\tCould not process document due to error: {e}")
        return None