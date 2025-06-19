-- 003_create_track_dimension_table.sql
-- Description: Creates the final TRACK_DIM table with SCD Type 2 tracking.

USE DATABASE SPOTIFY_DB;
USE SCHEMA DIMENSIONS;

CREATE TABLE IF NOT EXISTS TRACK_DIM (
    -- Surrogate Key
    TRACK_SK                BIGINT AUTOINCREMENT START 1 INCREMENT 1,
    
    -- Business Key
    TRACK_ID                VARCHAR(32) NOT NULL,
    
    -- Track Attributes
    TRACK_NAME              VARCHAR(255),
    DURATION_MS             INTEGER,
    IS_EXPLICIT             BOOLEAN,
    POPULARITY              INTEGER,
    PREVIEW_URL             VARCHAR(1024),
    
    -- Denormalized Album Attributes
    ALBUM_ID                VARCHAR(32),
    ALBUM_NAME              VARCHAR(255),
    ALBUM_RELEASE_DATE      DATE,
    ALBUM_TYPE              VARCHAR(50),
    
    -- Denormalized Artist Attributes
    PRIMARY_ARTIST_ID       VARCHAR(32),
    PRIMARY_ARTIST_NAME     VARCHAR(255),
    ALL_ARTIST_IDS          ARRAY,
    ALL_ARTIST_NAMES        ARRAY,

    -- SCD Type 2 Columns
    ROW_HASH                VARCHAR(64) NOT NULL, -- Hash of all attribute columns for change detection
    EFFECTIVE_START_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    EFFECTIVE_END_TIMESTAMP   TIMESTAMP_NTZ,
    IS_CURRENT_FLAG         BOOLEAN NOT NULL,
    
    -- Metadata Columns
    VERSION_NUMBER          SMALLINT,
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ,

    -- Primary Key Constraint
    CONSTRAINT PK_TRACK_DIM PRIMARY KEY (TRACK_SK)
);