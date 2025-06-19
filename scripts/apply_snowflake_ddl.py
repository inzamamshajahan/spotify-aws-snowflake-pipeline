import os
import snowflake.connector
from pathlib import Path

def get_snowflake_connection():
    """Establishes a connection to Snowflake using environment variables."""
    try:
        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            role=os.environ["SNOWFLAKE_ROLE"],
        )
        return conn
    except KeyError as e:
        raise KeyError(f"Environment variable {e} not set. Please set all required Snowflake credentials.")
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

def apply_ddl_scripts(conn, ddl_directory: Path):
    """Finds and applies all .sql DDL scripts in a directory in alphabetical order."""
    print(f"Searching for DDL scripts in: {ddl_directory}")
    
    # Get all .sql files and sort them to ensure execution order
    sql_files = sorted(ddl_directory.glob("*.sql"))

    if not sql_files:
        print("No .sql files found.")
        return

    try:
        for sql_file in sql_files:
            print(f"Applying script: {sql_file.name}...")
            with open(sql_file, "r") as f:
                sql_content = f.read()
                
                # --- THIS IS THE FIX ---
                # Call execute_string on the connection object (conn), not a cursor.
                # It returns an iterator of cursor objects, one for each statement.
                for cursor in conn.execute_string(sql_content):
                    # For DDL statements, we usually don't need the results, but it's good practice
                    # to confirm execution. We can fetch a row if the statement returns one.
                    result = cursor.fetchone()
                    print(f"  -> Statement executed. Result: {result if result else 'No rows returned.'}")
            print(f"Successfully applied {sql_file.name}")
    except Exception as e:
        # It's helpful to know which file failed
        print(f"ERROR applying DDL script '{sql_file.name}': {e}")
        raise # Re-raise the exception to stop the script

def main():
    """Main function to run the DDL application process."""
    conn = None
    try:
        # The DDL directory is one level up from this script, then into 'snowflake_ddl'
        script_location = Path(__file__).resolve().parent
        ddl_path = script_location.parent / "snowflake_ddl"
        
        conn = get_snowflake_connection()
        apply_ddl_scripts(conn, ddl_path)
        print("\nAll DDL scripts applied successfully!")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nSnowflake connection closed.")

if __name__ == "__main__":
    main()
