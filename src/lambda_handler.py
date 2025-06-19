# src/lambda_handler.py

import datetime
from typing import Dict, Any

from src.config import get_spotify_credentials
from src.auth import get_spotify_access_token
from src.spotify_client import get_new_releases, get_album_tracks
from src.s3_manager import upload_to_s3
from src.snowflake_manager import get_snowflake_connection, copy_into_staging, merge_scd2_logic

def handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """AWS Lambda handler function for the full ETL pipeline."""
    print("Pipeline execution started.")
    all_tracks = []
    
    try:
        # --- Stage 1: Extract data from Spotify ---
        credentials = get_spotify_credentials()
        access_token = get_spotify_access_token(
            credentials["spotify_client_id"], credentials["spotify_client_secret"]
        )
        
        new_release_albums = get_new_releases(access_token, limit=5) # Keep limit low for testing
        print(f"Found {len(new_release_albums)} new albums.")

        for album in new_release_albums:
            tracks = get_album_tracks(access_token, album['id'])
            all_tracks.extend(tracks)

        print(f"Total tracks fetched: {len(all_tracks)}")

        if not all_tracks:
            print("No new tracks found. Exiting.")
            return {"statusCode": 200, "body": "No new tracks to process."}

        # --- Stage 2: Load raw data into S3 ---
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"tracks_{timestamp}.jsonl"
        s3_folder = "raw/tracks" # Be more specific with the folder
        s3_key = f"{s3_folder}/{file_name}"

        # s3_manager.upload_to_s3 returns the S3 response, but we need the key for Snowflake
        upload_to_s3(data=all_tracks, file_name=file_name, folder=s3_folder)
        
        # --- Stage 3: Load and Transform data in Snowflake ---
        print("Connecting to Snowflake...")
        snowflake_conn = get_snowflake_connection()
        
        try:
            # Copy from S3 into Staging Table
            copy_into_staging(snowflake_conn, s3_key)
            
            # Apply SCD Type 2 logic from Staging to Dimension
            merge_scd2_logic(snowflake_conn)
        finally:
            snowflake_conn.close()
            print("Snowflake connection closed.")

        print("Pipeline execution finished successfully.")
        return {"statusCode": 200, "body": f"Successfully processed {len(all_tracks)} tracks."}

    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        # It's good practice to re-raise the exception to make the Lambda invocation fail
        raise e