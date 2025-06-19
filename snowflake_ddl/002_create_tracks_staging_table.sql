-- 002_create_tracks_staging_table.sql
-- Description: Creates the staging table for raw track data from S3.

USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW_LANDING;

CREATE TABLE IF NOT EXISTS TRACKS_STAGING (
    RAW_TRACK_DATA      VARIANT,          -- Column to hold the entire JSON object for a track.
    S3_FILE_PATH        VARCHAR(1024),    -- The full S3 path of the file this record came from.
    LOADED_AT           TIMESTAMP_NTZ     -- Timestamp when the record was loaded via COPY INTO.
);