# src/snowflake_manager.py

import os
import json
import snowflake.connector
from typing import Dict, Any

def get_snowflake_creds() -> Dict[str, Any]:
    """Retrieves Snowflake credentials from AWS Secrets Manager."""
    import boto3
    secret_name = os.environ["SNOWFLAKE_SECRET_NAME"]

    # Boto3 will automatically detect the region when running in Lambda
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)

def get_snowflake_connection():
    """Establishes a connection to Snowflake using credentials from Secrets Manager."""
    creds = get_snowflake_creds()
    return snowflake.connector.connect(
        user=creds["snowflake_user"],
        password=creds["snowflake_password"],
        account=creds["snowflake_account"],
        warehouse=creds["snowflake_warehouse"],
        database=creds["snowflake_database"],
        role=creds["snowflake_role"],
        schema="RAW_LANDING" # Default schema
    )

def copy_into_staging(conn, s3_key: str):
    """Copies data from a specific S3 file into the staging table."""
    full_s3_path = f"s3://{os.environ['S3_BUCKET_NAME']}/{s3_key}"
    
    # The stage name and storage integration name must match what you created in Snowflake
    stage_name = "spotify_s3_stage"
    storage_integration_name = "s3_spotify_integration"
    
    copy_sql = f"""
    COPY INTO SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING(RAW_TRACK_DATA, S3_FILE_PATH, LOADED_AT)
    FROM (
        SELECT
            $1,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @{stage_name}/{s3_key}
    )
    FILE_FORMAT = (TYPE = 'JSON')
    ON_ERROR = 'SKIP_FILE';
    """
    
    print("Executing COPY INTO command...")
    cursor = conn.cursor()
    cursor.execute("USE WAREHOUSE ANALYTICS_WH;")
    
    # Create a named stage that uses our storage integration
    create_stage_sql = f"""
    CREATE OR REPLACE STAGE {stage_name}
      URL='s3://{os.environ['S3_BUCKET_NAME']}/' 
      STORAGE_INTEGRATION = {storage_integration_name};
    """
    cursor.execute(create_stage_sql)
    
    # Execute the copy command
    cursor.execute(copy_sql)
    cursor.close()
    print(f"Successfully copied data from {full_s3_path} into TRACKS_STAGING.")


def merge_scd2_logic(conn):
    """Executes the MERGE statement for SCD Type 2 logic."""
    
    scd2_sql = """
    -- Step 1: MERGE to update existing records that have changed and insert new records.
    MERGE INTO SPOTIFY_DB.DIMENSIONS.TRACK_DIM TGT
    USING (
        -- Subquery to select, flatten, and hash the latest data from staging
        WITH LATEST_STAGED_TRACKS AS (
            SELECT
                s.RAW_TRACK_DATA:id::VARCHAR AS track_id,
                ROW_NUMBER() OVER(PARTITION BY s.RAW_TRACK_DATA:id::VARCHAR ORDER BY s.LOADED_AT DESC) as rn
            FROM SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING s
        ),
        STAGED_DATA AS (
            SELECT 
                s.RAW_TRACK_DATA:id::VARCHAR AS track_id,
                s.RAW_TRACK_DATA:name::VARCHAR AS track_name,
                s.RAW_TRACK_DATA:duration_ms::INTEGER AS duration_ms,
                s.RAW_TRACK_DATA:explicit::BOOLEAN AS is_explicit,
                s.RAW_TRACK_DATA:popularity::INTEGER AS popularity,
                s.RAW_TRACK_DATA:preview_url::VARCHAR AS preview_url,
                s.RAW_TRACK_DATA:album:id::VARCHAR AS album_id,
                s.RAW_TRACK_DATA:album:name::VARCHAR AS album_name,
                s.RAW_TRACK_DATA:album:release_date::DATE AS album_release_date,
                s.RAW_TRACK_DATA:album:album_type::VARCHAR AS album_type,
                s.RAW_TRACK_DATA:artists[0]:id::VARCHAR AS primary_artist_id,
                s.RAW_TRACK_DATA:artists[0]:name::VARCHAR AS primary_artist_name,
                s.RAW_TRACK_DATA:artists AS all_artists, -- Keep as VARIANT for now
                MD5(CONCAT_WS('||', 
                    track_name, IFNULL(duration_ms, ''), IFNULL(is_explicit, ''), IFNULL(popularity, ''), 
                    IFNULL(preview_url, ''), album_id, primary_artist_id
                )) as row_hash,
                s.LOADED_AT
            FROM SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING s
            JOIN LATEST_STAGED_TRACKS l ON s.RAW_TRACK_DATA:id::VARCHAR = l.track_id AND l.rn = 1
        )
        SELECT *,
            ARRAY_CONSTRUCT_COMPACT(all_artists[0]:id::VARCHAR, all_artists[1]:id::VARCHAR, all_artists[2]:id::VARCHAR) as all_artist_ids,
            ARRAY_CONSTRUCT_COMPACT(all_artists[0]:name::VARCHAR, all_artists[1]:name::VARCHAR, all_artists[2]:name::VARCHAR) as all_artist_names
        FROM STAGED_DATA
    ) SRC
    ON TGT.TRACK_ID = SRC.track_id AND TGT.IS_CURRENT_FLAG = TRUE
    
    -- WHEN MATCHED for changed records: Expire the old record
    WHEN MATCHED AND TGT.ROW_HASH <> SRC.row_hash THEN
        UPDATE SET
            TGT.EFFECTIVE_END_TIMESTAMP = SRC.LOADED_AT,
            TGT.IS_CURRENT_FLAG = FALSE
    
    -- WHEN NOT MATCHED for new records: Insert the new record
    WHEN NOT MATCHED THEN
        INSERT (
            TRACK_ID, TRACK_NAME, DURATION_MS, IS_EXPLICIT, POPULARITY, PREVIEW_URL,
            ALBUM_ID, ALBUM_NAME, ALBUM_RELEASE_DATE, ALBUM_TYPE,
            PRIMARY_ARTIST_ID, PRIMARY_ARTIST_NAME, ALL_ARTIST_IDS, ALL_ARTIST_NAMES,
            ROW_HASH, EFFECTIVE_START_TIMESTAMP, EFFECTIVE_END_TIMESTAMP, IS_CURRENT_FLAG, VERSION_NUMBER, UPDATED_AT
        ) VALUES (
            SRC.track_id, SRC.track_name, SRC.duration_ms, SRC.is_explicit, SRC.popularity, SRC.preview_url,
            SRC.album_id, SRC.album_name, SRC.album_release_date, SRC.album_type,
            SRC.primary_artist_id, SRC.primary_artist_name, SRC.all_artist_ids, SRC.all_artist_names,
            SRC.row_hash, SRC.LOADED_AT, NULL, TRUE, 1, CURRENT_TIMESTAMP()
        );

    -- Step 2: INSERT new versions for changed records. This needs to run in a separate transaction.
    -- (The MERGE statement above handles this logic for new and updated records, but this separate INSERT is a robust pattern for complex SCD2)
    -- We can simplify this logic by having a single MERGE statement.
    -- For simplicity and robustness, we will do this in two steps: Update expires, then insert new versions.
    
    -- We will re-write this part to be a two-step process in two separate execute calls
    """
    
    print("Executing SCD Type 2 logic...")
    cursor = conn.cursor()
    cursor.execute("USE WAREHOUSE ANALYTICS_WH;")
    
    # Snowflake's MERGE can't update a row and insert a new one based on it in the same statement.
    # So we run two statements.
    
    # Statement 1: Expire old records and insert completely new ones
    cursor.execute(scd2_sql)
    print("MERGE statement completed. Expired old records and inserted new ones.")
    
    # Statement 2: Insert the *new version* of records that were just expired.
    insert_updated_sql = """
    INSERT INTO SPOTIFY_DB.DIMENSIONS.TRACK_DIM (
        TRACK_ID, TRACK_NAME, DURATION_MS, IS_EXPLICIT, POPULARITY, PREVIEW_URL,
        ALBUM_ID, ALBUM_NAME, ALBUM_RELEASE_DATE, ALBUM_TYPE,
        PRIMARY_ARTIST_ID, PRIMARY_ARTIST_NAME, ALL_ARTIST_IDS, ALL_ARTIST_NAMES,
        ROW_HASH, EFFECTIVE_START_TIMESTAMP, EFFECTIVE_END_TIMESTAMP, IS_CURRENT_FLAG, VERSION_NUMBER, UPDATED_AT
    )
    SELECT
        SRC.track_id, SRC.track_name, SRC.duration_ms, SRC.is_explicit, SRC.popularity, SRC.preview_url,
        SRC.album_id, SRC.album_name, SRC.album_release_date, SRC.album_type,
        SRC.primary_artist_id, SRC.primary_artist_name, SRC.all_artist_ids, SRC.all_artist_names,
        SRC.row_hash,
        SRC.LOADED_AT AS EFFECTIVE_START_TIMESTAMP,
        NULL AS EFFECTIVE_END_TIMESTAMP,
        TRUE AS IS_CURRENT_FLAG,
        TGT.VERSION_NUMBER + 1 AS VERSION_NUMBER,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM (
        WITH LATEST_STAGED_TRACKS AS (
            SELECT
                s.RAW_TRACK_DATA:id::VARCHAR AS track_id,
                ROW_NUMBER() OVER(PARTITION BY s.RAW_TRACK_DATA:id::VARCHAR ORDER BY s.LOADED_AT DESC) as rn
            FROM SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING s
        ),
        STAGED_DATA AS (
            SELECT 
                s.RAW_TRACK_DATA:id::VARCHAR AS track_id, s.RAW_TRACK_DATA:name::VARCHAR AS track_name,
                s.RAW_TRACK_DATA:duration_ms::INTEGER AS duration_ms, s.RAW_TRACK_DATA:explicit::BOOLEAN AS is_explicit,
                s.RAW_TRACK_DATA:popularity::INTEGER AS popularity, s.RAW_TRACK_DATA:preview_url::VARCHAR AS preview_url,
                s.RAW_TRACK_DATA:album:id::VARCHAR AS album_id, s.RAW_TRACK_DATA:album:name::VARCHAR AS album_name,
                s.RAW_TRACK_DATA:album:release_date::DATE AS album_release_date, s.RAW_TRACK_DATA:album:album_type::VARCHAR AS album_type,
                s.RAW_TRACK_DATA:artists[0]:id::VARCHAR AS primary_artist_id, s.RAW_TRACK_DATA:artists[0]:name::VARCHAR AS primary_artist_name,
                s.RAW_TRACK_DATA:artists AS all_artists,
                MD5(CONCAT_WS('||', track_name, IFNULL(duration_ms, ''), IFNULL(is_explicit, ''), IFNULL(popularity, ''), IFNULL(preview_url, ''), album_id, primary_artist_id)) as row_hash,
                s.LOADED_AT
            FROM SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING s
            JOIN LATEST_STAGED_TRACKS l ON s.RAW_TRACK_DATA:id::VARCHAR = l.track_id AND l.rn = 1
        )
        SELECT *,
            ARRAY_CONSTRUCT_COMPACT(all_artists[0]:id::VARCHAR, all_artists[1]:id::VARCHAR, all_artists[2]:id::VARCHAR) as all_artist_ids,
            ARRAY_CONSTRUCT_COMPACT(all_artists[0]:name::VARCHAR, all_artists[1]:name::VARCHAR, all_artists[2]:name::VARCHAR) as all_artist_names
        FROM STAGED_DATA
    ) SRC
    JOIN SPOTIFY_DB.DIMENSIONS.TRACK_DIM TGT 
        ON SRC.track_id = TGT.TRACK_ID 
        AND TGT.ROW_HASH <> SRC.row_hash -- Find records with changes
        AND TGT.EFFECTIVE_END_TIMESTAMP IS NOT NULL; -- Whose old version was just expired
    """
    cursor.execute(insert_updated_sql)
    print("INSERT statement completed. Added new versions for updated records.")
    
    # Finally, clean up the staging table
    cursor.execute("TRUNCATE TABLE SPOTIFY_DB.RAW_LANDING.TRACKS_STAGING;")
    print("Staging table truncated.")
    
    cursor.close()
    print("SCD Type 2 logic completed successfully.")