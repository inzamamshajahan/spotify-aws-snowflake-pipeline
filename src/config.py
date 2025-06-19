# src/config.py
import os
import json
import boto3
from botocore.exceptions import ClientError

# --- Configuration Constants (read from environment variables)
# These will be set in the Lambda function's configuration
SECRET_NAME = os.environ.get("SPOTIFY_SECRET_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# --- Boto3 Clients
# Boto3 will automatically detect the region when running in Lambda.
session = boto3.session.Session()
secrets_manager_client = session.client(service_name="secretsmanager")
s3_client = session.client("s3")

def get_spotify_credentials() -> dict:
    """Retrieves Spotify API credentials from AWS Secrets Manager."""
    if not SECRET_NAME:
        raise ValueError("Environment variable SPOTIFY_SECRET_NAME is not set.")
    
    try:
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except ClientError as e:
        print(f"Error retrieving secret '{SECRET_NAME}': {e}")
        raise e