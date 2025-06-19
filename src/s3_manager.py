import json
from src.config import s3_client, S3_BUCKET_NAME

def upload_to_s3(data: list, file_name: str, folder: str = "raw") -> dict:
    """Uploads a list of dictionaries as a JSON file to S3."""
    if not S3_BUCKET_NAME:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set.")

    s3_key = f"{folder}/{file_name}"
    
    # Convert list of tracks to a JSON string
    # Using newline-delimited JSON (JSONL) is a best practice for data ingestion
    jsonl_data = "\n".join(json.dumps(record) for record in data)

    response = s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=jsonl_data,
        ContentType="application/jsonl"
    )
    print(f"Successfully uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")
    return response