import boto3
import pandas as pd
import io
import json
from config import AWS_REGION, BUCKET_NAME

s3_client = boto3.client('s3', region_name=AWS_REGION)

def extract(source_name, source_type, s3_key):
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    content = response['Body'].read()
    
    if source_type == 'csv':
        return pd.read_csv(io.BytesIO(content))
    elif source_type == 'json':
        return pd.read_json(io.StringIO(content.decode('utf-8')))