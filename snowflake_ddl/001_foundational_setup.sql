-- 001_foundational_setup.sql
-- Description: Creates the database, schemas, and warehouse for the project.

-- Use a role that has permissions to create databases and warehouses.
USE ROLE SYSADMIN;

-- Create the database for our Spotify project
CREATE DATABASE IF NOT EXISTS SPOTIFY_DB
  COMMENT = 'Database for the Spotify data pipeline project.';

-- Use the new database
USE DATABASE SPOTIFY_DB;

-- Create the schema for raw, unprocessed data
CREATE SCHEMA IF NOT EXISTS RAW_LANDING
  COMMENT = 'Schema for raw data landed from S3.';

-- Create the schema for transformed, dimensional data
CREATE SCHEMA IF NOT EXISTS DIMENSIONS
  COMMENT = 'Schema for curated dimension tables (e.g., SCD Type 2).';

-- Create a virtual warehouse for processing and analytics
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
  WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60 -- Suspend after 60 seconds of inactivity
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for running data pipeline and analytics queries.';