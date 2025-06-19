## README.md
```markdown

```

## cloudformation.yml
```yml
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  Deploys the Serverless Spotify Data Pipeline, including the Lambda function,
  IAM role, and a Lambda Layer for dependencies.

Parameters:
  ProjectName:
    Type: String
    Description: A prefix for all created resources.
    Default: 'SpotifySnowflakePipeline'
  
  DeploymentS3Bucket:
    Type: String
    Description: The S3 bucket where deployment artifacts (zips) are stored.

  RawDataS3BucketName:
    Type: String
    Description: The S3 bucket name for landing raw Spotify data.

  SpotifySecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Spotify credentials.
    Default: 'spotify/prod/api_credentials'

  SnowflakeSecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Snowflake credentials.
    Default: 'spotify_pipeline/prod/snowflake_credentials'

  # --- NEW PARAMETERS ---
  FunctionZipVersionId:
    Type: String
    Description: The S3 object version ID for the function.zip file.
  
  LayerZipVersionId:
    Type: String
    Description: The S3 object version ID for the lambda_layer.zip file.

Resources:
  # IAM Role for the Lambda Function
  LambdaExecutionRole:
    Type: 'AWS::IAM::Role'
    Properties:
      RoleName: !Sub '${ProjectName}-LambdaExecutionRole'
      # ... (rest of the role is unchanged) ...
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: 'lambda.amazonaws.com'
            Action: 'sts:AssumeRole'
      Policies:
        - PolicyName: !Sub '${ProjectName}-LambdaPermissions'
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - 'logs:CreateLogGroup'
                  - 'logs:CreateLogStream'
                  - 'logs:PutLogEvents'
                Resource: 'arn:aws:logs:*:*:*'
              - Effect: Allow
                Action: 'secretsmanager:GetSecretValue'
                Resource: 
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SpotifySecretName}-*'
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SnowflakeSecretName}-*'
              - Effect: Allow
                Action: 's3:PutObject'
                Resource: !Sub 'arn:aws:s3:::${RawDataS3BucketName}/raw/tracks/*'

  # Lambda Layer for Python Dependencies
  DependenciesLayer:
    Type: 'AWS::Lambda::LayerVersion'
    Properties:
      LayerName: !Sub '${ProjectName}-DependenciesLayer'
      Description: 'Dependencies for the Spotify pipeline (requests, snowflake-connector).'
      Content:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'lambda_layer.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref LayerZipVersionId
      CompatibleRuntimes:
        - 'python3.9'
        - 'python3.10'
      LicenseInfo: 'MIT'

  # The main Lambda Function
  SpotifyPipelineFunction:
    Type: 'AWS::Lambda::Function'
    Properties:
      FunctionName: !Sub '${ProjectName}-Function'
      Handler: 'src.lambda_handler.handler' # This should now be correct
      Role: !GetAtt LambdaExecutionRole.Arn
      Runtime: 'python3.9'
      Timeout: 90
      MemorySize: 256
      Code:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'function.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref FunctionZipVersionId
      Environment:
        Variables:
          S3_BUCKET_NAME: !Ref RawDataS3BucketName
          SPOTIFY_SECRET_NAME: !Ref SpotifySecretName
          SNOWFLAKE_SECRET_NAME: !Ref SnowflakeSecretName
      Layers:
        - !Ref DependenciesLayer

Outputs:
  LambdaFunctionName:
    Description: "The name of the created Lambda function."
    Value: !Ref SpotifyPipelineFunction
```

## requirements-dev.txt
```text
-r requirements.txt
pytest
pytest-mock
moto[s3,secretsmanager]
black
snowflake-connector-python
```

## requirements.txt
```text
boto3
requests
snowflake-connector-python
```

## deploy.sh
```
#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
DEPLOYMENT_S3_BUCKET="$1"
STACK_NAME="spotify-snowflake-pipeline-stack"
RAW_DATA_S3_BUCKET="$2"
LAYER_S3_KEY="lambda_layer.zip"
FUNCTION_S3_KEY="function.zip"

if [ -z "$DEPLOYMENT_S3_BUCKET" ] || [ -z "$RAW_DATA_S3_BUCKET" ]; then
  echo "Usage: ./deploy.sh <deployment_s3_bucket> <raw_data_s_bucket>"
  exit 1
fi

echo "Starting deployment..."

# --- Pre-Deployment Check for ROLLBACK_COMPLETE state ---
echo "Checking status of stack: $STACK_NAME"
STATUS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
if [ "$STATUS" == "ROLLBACK_COMPLETE" ]; then
  echo "Stack is in ROLLBACK_COMPLETE state. Deleting stack before deployment."
  aws cloudformation delete-stack --stack-name $STACK_NAME
  echo "Waiting for stack deletion to complete..."
  aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME
  echo "Stack deleted successfully."
fi

# --- 1. Package Lambda Layer by forcing pip to use Lambda-compatible wheels ---
echo "Packaging Lambda layer using Lambda-compatible wheels..."
# Clean up previous build directory
rm -rf build
mkdir -p build/lambda_layer/python

# This command tells pip to download pre-compiled binaries ("wheels") that are
# compatible with the 'manylinux2014_x86_64' platform, which works with AWS Lambda.
pip install \
    -r requirements.txt \
    -t build/lambda_layer/python/ \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.9 \
    --only-binary=:all: --upgrade

# Now zip the contents of the 'python' directory
cd build/lambda_layer
zip -r ../../lambda_layer.zip .
cd ../..
echo "Lambda layer packaged successfully."


# --- 2. Package Lambda Function Code ---
echo "Packaging Lambda function code..."
zip -r function.zip src -x "src/__pycache__/*"
echo "Lambda function code packaged successfully."

# --- 3. Upload Artifacts to S3 ---
echo "Uploading artifacts to S3 bucket: $DEPLOYMENT_S3_BUCKET"
aws s3 cp lambda_layer.zip s3://$DEPLOYMENT_S3_BUCKET/$LAYER_S3_KEY
aws s3 cp function.zip s3://$DEPLOYMENT_S3_BUCKET/$FUNCTION_S3_KEY
echo "Artifacts uploaded successfully."

# --- Get the Version IDs of the uploaded files ---
echo "Retrieving object version IDs..."
LAYER_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $LAYER_S3_KEY --query 'VersionId' --output text)
FUNCTION_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $FUNCTION_S3_KEY --query 'VersionId' --output text)
echo "  -> Layer Version ID: $LAYER_VERSION_ID"
echo "  -> Function Version ID: $FUNCTION_VERSION_ID"

# --- 4. Deploy CloudFormation Stack ---
echo "Deploying CloudFormation stack: $STACK_NAME"
aws cloudformation deploy \
  --template-file cloudformation.yml \
  --stack-name $STACK_NAME \
  --parameter-overrides \
    DeploymentS3Bucket=$DEPLOYMENT_S3_BUCKET \
    RawDataS3BucketName=$RAW_DATA_S3_BUCKET \
    FunctionZipVersionId=$FUNCTION_VERSION_ID \
    LayerZipVersionId=$LAYER_VERSION_ID \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset

echo "Deployment complete."

# Clean up local build artifacts
rm -rf build lambda_layer.zip function.zip
```

## project_overview.md
```markdown
## README.md
```markdown

```

## cloudformation.yml
```yml
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  Deploys the Serverless Spotify Data Pipeline, including the Lambda function,
  IAM role, and a Lambda Layer for dependencies.

Parameters:
  ProjectName:
    Type: String
    Description: A prefix for all created resources.
    Default: 'SpotifySnowflakePipeline'
  
  DeploymentS3Bucket:
    Type: String
    Description: The S3 bucket where deployment artifacts (zips) are stored.

  RawDataS3BucketName:
    Type: String
    Description: The S3 bucket name for landing raw Spotify data.

  SpotifySecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Spotify credentials.
    Default: 'spotify/prod/api_credentials'

  SnowflakeSecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Snowflake credentials.
    Default: 'spotify_pipeline/prod/snowflake_credentials'

  # --- NEW PARAMETERS ---
  FunctionZipVersionId:
    Type: String
    Description: The S3 object version ID for the function.zip file.
  
  LayerZipVersionId:
    Type: String
    Description: The S3 object version ID for the lambda_layer.zip file.

Resources:
  # IAM Role for the Lambda Function
  LambdaExecutionRole:
    Type: 'AWS::IAM::Role'
    Properties:
      RoleName: !Sub '${ProjectName}-LambdaExecutionRole'
      # ... (rest of the role is unchanged) ...
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: 'lambda.amazonaws.com'
            Action: 'sts:AssumeRole'
      Policies:
        - PolicyName: !Sub '${ProjectName}-LambdaPermissions'
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - 'logs:CreateLogGroup'
                  - 'logs:CreateLogStream'
                  - 'logs:PutLogEvents'
                Resource: 'arn:aws:logs:*:*:*'
              - Effect: Allow
                Action: 'secretsmanager:GetSecretValue'
                Resource: 
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SpotifySecretName}-*'
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SnowflakeSecretName}-*'
              - Effect: Allow
                Action: 's3:PutObject'
                Resource: !Sub 'arn:aws:s3:::${RawDataS3BucketName}/raw/tracks/*'

  # Lambda Layer for Python Dependencies
  DependenciesLayer:
    Type: 'AWS::Lambda::LayerVersion'
    Properties:
      LayerName: !Sub '${ProjectName}-DependenciesLayer'
      Description: 'Dependencies for the Spotify pipeline (requests, snowflake-connector).'
      Content:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'lambda_layer.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref LayerZipVersionId
      CompatibleRuntimes:
        - 'python3.9'
        - 'python3.10'
      LicenseInfo: 'MIT'

  # The main Lambda Function
  SpotifyPipelineFunction:
    Type: 'AWS::Lambda::Function'
    Properties:
      FunctionName: !Sub '${ProjectName}-Function'
      Handler: 'src.lambda_handler.handler' # This should now be correct
      Role: !GetAtt LambdaExecutionRole.Arn
      Runtime: 'python3.9'
      Timeout: 90
      MemorySize: 256
      Code:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'function.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref FunctionZipVersionId
      Environment:
        Variables:
          S3_BUCKET_NAME: !Ref RawDataS3BucketName
          SPOTIFY_SECRET_NAME: !Ref SpotifySecretName
          SNOWFLAKE_SECRET_NAME: !Ref SnowflakeSecretName
      Layers:
        - !Ref DependenciesLayer

Outputs:
  LambdaFunctionName:
    Description: "The name of the created Lambda function."
    Value: !Ref SpotifyPipelineFunction
```

## requirements-dev.txt
```text
-r requirements.txt
pytest
pytest-mock
moto[s3,secretsmanager]
black
snowflake-connector-python
```

## requirements.txt
```text
boto3
requests
snowflake-connector-python
```

## deploy.sh
```
#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
DEPLOYMENT_S3_BUCKET="$1"
STACK_NAME="spotify-snowflake-pipeline-stack"
RAW_DATA_S3_BUCKET="$2"
LAYER_S3_KEY="lambda_layer.zip"
FUNCTION_S3_KEY="function.zip"

if [ -z "$DEPLOYMENT_S3_BUCKET" ] || [ -z "$RAW_DATA_S3_BUCKET" ]; then
  echo "Usage: ./deploy.sh <deployment_s3_bucket> <raw_data_s_bucket>"
  exit 1
fi

echo "Starting deployment..."

# --- Pre-Deployment Check for ROLLBACK_COMPLETE state ---
echo "Checking status of stack: $STACK_NAME"
STATUS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
if [ "$STATUS" == "ROLLBACK_COMPLETE" ]; then
  echo "Stack is in ROLLBACK_COMPLETE state. Deleting stack before deployment."
  aws cloudformation delete-stack --stack-name $STACK_NAME
  echo "Waiting for stack deletion to complete..."
  aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME
  echo "Stack deleted successfully."
fi

# --- 1. Package Lambda Layer by forcing pip to use Lambda-compatible wheels ---
echo "Packaging Lambda layer using Lambda-compatible wheels..."
# Clean up previous build directory
rm -rf build
mkdir -p build/lambda_layer/python

# This command tells pip to download pre-compiled binaries ("wheels") that are
# compatible with the 'manylinux2014_x86_64' platform, which works with AWS Lambda.
pip install \
    -r requirements.txt \
    -t build/lambda_layer/python/ \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.9 \
    --only-binary=:all: --upgrade

# Now zip the contents of the 'python' directory
cd build/lambda_layer
zip -r ../../lambda_layer.zip .
cd ../..
echo "Lambda layer packaged successfully."


# --- 2. Package Lambda Function Code ---
echo "Packaging Lambda function code..."
zip -r function.zip src -x "src/__pycache__/*"
echo "Lambda function code packaged successfully."

# --- 3. Upload Artifacts to S3 ---
echo "Uploading artifacts to S3 bucket: $DEPLOYMENT_S3_BUCKET"
aws s3 cp lambda_layer.zip s3://$DEPLOYMENT_S3_BUCKET/$LAYER_S3_KEY
aws s3 cp function.zip s3://$DEPLOYMENT_S3_BUCKET/$FUNCTION_S3_KEY
echo "Artifacts uploaded successfully."

# --- Get the Version IDs of the uploaded files ---
echo "Retrieving object version IDs..."
LAYER_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $LAYER_S3_KEY --query 'VersionId' --output text)
FUNCTION_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $FUNCTION_S3_KEY --query 'VersionId' --output text)
echo "  -> Layer Version ID: $LAYER_VERSION_ID"
echo "  -> Function Version ID: $FUNCTION_VERSION_ID"

# --- 4. Deploy CloudFormation Stack ---
echo "Deploying CloudFormation stack: $STACK_NAME"
aws cloudformation deploy \
  --template-file cloudformation.yml \
  --stack-name $STACK_NAME \
  --parameter-overrides \
    DeploymentS3Bucket=$DEPLOYMENT_S3_BUCKET \
    RawDataS3BucketName=$RAW_DATA_S3_BUCKET \
    FunctionZipVersionId=$FUNCTION_VERSION_ID \
    LayerZipVersionId=$LAYER_VERSION_ID \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset

echo "Deployment complete."

# Clean up local build artifacts
rm -rf build lambda_layer.zip function.zip
```

## project_overview.md
```markdown
## README.md
```markdown

```

## cloudformation.yml
```yml
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  Deploys the Serverless Spotify Data Pipeline, including the Lambda function,
  IAM role, and a Lambda Layer for dependencies.

Parameters:
  ProjectName:
    Type: String
    Description: A prefix for all created resources.
    Default: 'SpotifySnowflakePipeline'
  
  DeploymentS3Bucket:
    Type: String
    Description: The S3 bucket where deployment artifacts (zips) are stored.

  RawDataS3BucketName:
    Type: String
    Description: The S3 bucket name for landing raw Spotify data.

  SpotifySecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Spotify credentials.
    Default: 'spotify/prod/api_credentials'

  SnowflakeSecretName:
    Type: String
    Description: The name of the secret in AWS Secrets Manager for Snowflake credentials.
    Default: 'spotify_pipeline/prod/snowflake_credentials'

  # --- NEW PARAMETERS ---
  FunctionZipVersionId:
    Type: String
    Description: The S3 object version ID for the function.zip file.
  
  LayerZipVersionId:
    Type: String
    Description: The S3 object version ID for the lambda_layer.zip file.

Resources:
  # IAM Role for the Lambda Function
  LambdaExecutionRole:
    Type: 'AWS::IAM::Role'
    Properties:
      RoleName: !Sub '${ProjectName}-LambdaExecutionRole'
      # ... (rest of the role is unchanged) ...
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: 'lambda.amazonaws.com'
            Action: 'sts:AssumeRole'
      Policies:
        - PolicyName: !Sub '${ProjectName}-LambdaPermissions'
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - 'logs:CreateLogGroup'
                  - 'logs:CreateLogStream'
                  - 'logs:PutLogEvents'
                Resource: 'arn:aws:logs:*:*:*'
              - Effect: Allow
                Action: 'secretsmanager:GetSecretValue'
                Resource: 
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SpotifySecretName}-*'
                  - !Sub 'arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:${SnowflakeSecretName}-*'
              - Effect: Allow
                Action: 's3:PutObject'
                Resource: !Sub 'arn:aws:s3:::${RawDataS3BucketName}/raw/tracks/*'

  # Lambda Layer for Python Dependencies
  DependenciesLayer:
    Type: 'AWS::Lambda::LayerVersion'
    Properties:
      LayerName: !Sub '${ProjectName}-DependenciesLayer'
      Description: 'Dependencies for the Spotify pipeline (requests, snowflake-connector).'
      Content:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'lambda_layer.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref LayerZipVersionId
      CompatibleRuntimes:
        - 'python3.9'
        - 'python3.10'
      LicenseInfo: 'MIT'

  # The main Lambda Function
  SpotifyPipelineFunction:
    Type: 'AWS::Lambda::Function'
    Properties:
      FunctionName: !Sub '${ProjectName}-Function'
      Handler: 'src.lambda_handler.handler' # This should now be correct
      Role: !GetAtt LambdaExecutionRole.Arn
      Runtime: 'python3.9'
      Timeout: 90
      MemorySize: 256
      Code:
        S3Bucket: !Ref DeploymentS3Bucket
        S3Key: 'function.zip'
        # --- MODIFIED: Use the specific version ID ---
        S3ObjectVersion: !Ref FunctionZipVersionId
      Environment:
        Variables:
          S3_BUCKET_NAME: !Ref RawDataS3BucketName
          SPOTIFY_SECRET_NAME: !Ref SpotifySecretName
          SNOWFLAKE_SECRET_NAME: !Ref SnowflakeSecretName
      Layers:
        - !Ref DependenciesLayer

Outputs:
  LambdaFunctionName:
    Description: "The name of the created Lambda function."
    Value: !Ref SpotifyPipelineFunction
```

## requirements-dev.txt
```text
-r requirements.txt
pytest
pytest-mock
moto[s3,secretsmanager]
black
snowflake-connector-python
```

## requirements.txt
```text
boto3
requests
snowflake-connector-python
```

## deploy.sh
```
#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
DEPLOYMENT_S3_BUCKET="$1"
STACK_NAME="spotify-snowflake-pipeline-stack"
RAW_DATA_S3_BUCKET="$2"
LAYER_S3_KEY="lambda_layer.zip"
FUNCTION_S3_KEY="function.zip"

if [ -z "$DEPLOYMENT_S3_BUCKET" ] || [ -z "$RAW_DATA_S3_BUCKET" ]; then
  echo "Usage: ./deploy.sh <deployment_s3_bucket> <raw_data_s_bucket>"
  exit 1
fi

echo "Starting deployment..."

# --- Pre-Deployment Check for ROLLBACK_COMPLETE state ---
echo "Checking status of stack: $STACK_NAME"
STATUS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
if [ "$STATUS" == "ROLLBACK_COMPLETE" ]; then
  echo "Stack is in ROLLBACK_COMPLETE state. Deleting stack before deployment."
  aws cloudformation delete-stack --stack-name $STACK_NAME
  echo "Waiting for stack deletion to complete..."
  aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME
  echo "Stack deleted successfully."
fi

# --- 1. Package Lambda Layer by forcing pip to use Lambda-compatible wheels ---
echo "Packaging Lambda layer using Lambda-compatible wheels..."
# Clean up previous build directory
rm -rf build
mkdir -p build/lambda_layer/python

# This command tells pip to download pre-compiled binaries ("wheels") that are
# compatible with the 'manylinux2014_x86_64' platform, which works with AWS Lambda.
pip install \
    -r requirements.txt \
    -t build/lambda_layer/python/ \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.9 \
    --only-binary=:all: --upgrade

# Now zip the contents of the 'python' directory
cd build/lambda_layer
zip -r ../../lambda_layer.zip .
cd ../..
echo "Lambda layer packaged successfully."


# --- 2. Package Lambda Function Code ---
echo "Packaging Lambda function code..."
zip -r function.zip src -x "src/__pycache__/*"
echo "Lambda function code packaged successfully."

# --- 3. Upload Artifacts to S3 ---
echo "Uploading artifacts to S3 bucket: $DEPLOYMENT_S3_BUCKET"
aws s3 cp lambda_layer.zip s3://$DEPLOYMENT_S3_BUCKET/$LAYER_S3_KEY
aws s3 cp function.zip s3://$DEPLOYMENT_S3_BUCKET/$FUNCTION_S3_KEY
echo "Artifacts uploaded successfully."

# --- Get the Version IDs of the uploaded files ---
echo "Retrieving object version IDs..."
LAYER_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $LAYER_S3_KEY --query 'VersionId' --output text)
FUNCTION_VERSION_ID=$(aws s3api head-object --bucket $DEPLOYMENT_S3_BUCKET --key $FUNCTION_S3_KEY --query 'VersionId' --output text)
echo "  -> Layer Version ID: $LAYER_VERSION_ID"
echo "  -> Function Version ID: $FUNCTION_VERSION_ID"

# --- 4. Deploy CloudFormation Stack ---
echo "Deploying CloudFormation stack: $STACK_NAME"
aws cloudformation deploy \
  --template-file cloudformation.yml \
  --stack-name $STACK_NAME \
  --parameter-overrides \
    DeploymentS3Bucket=$DEPLOYMENT_S3_BUCKET \
    RawDataS3BucketName=$RAW_DATA_S3_BUCKET \
    FunctionZipVersionId=$FUNCTION_VERSION_ID \
    LayerZipVersionId=$LAYER_VERSION_ID \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset

echo "Deployment complete."

# Clean up local build artifacts
rm -rf build lambda_layer.zip function.zip
```

## scripts/apply_snowflake_ddl.py
```python
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

```

## snowflake_ddl/001_foundational_setup.sql
```
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
```

## snowflake_ddl/002_create_tracks_staging_table.sql
```
-- 002_create_tracks_staging_table.sql
-- Description: Creates the staging table for raw track data from S3.

USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW_LANDING;

CREATE TABLE IF NOT EXISTS TRACKS_STAGING (
    RAW_TRACK_DATA      VARIANT,          -- Column to hold the entire JSON object for a track.
    S3_FILE_PATH        VARCHAR(1024),    -- The full S3 path of the file this record came from.
    LOADED_AT           TIMESTAMP_NTZ     -- Timestamp when the record was loaded via COPY INTO.
);
```

## snowflake_ddl/003_create_track_dimension_table.sql
```
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
```

## src/__init__.py
```python

```

## src/auth.py
```python
import requests
from requests.auth import HTTPBasicAuth

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"

def get_spotify_access_token(client_id: str, client_secret: str) -> str:
    """Obtains an access token from the Spotify API."""
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    
    response = requests.post(SPOTIFY_AUTH_URL, auth=auth, data=data)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    
    token_info = response.json()
    return token_info["access_token"]
```

## src/config.py
```python
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
```

## src/lambda_handler.py
```python
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
```

## src/s3_manager.py
```python
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
```

## src/snowflake_manager.py
```python
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
```

## src/spotify_client.py
```python
import requests
from typing import List, Dict, Any

SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

def get_new_releases(access_token: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Fetches newly released albums from Spotify."""
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": limit}
    response = requests.get(f"{SPOTIFY_API_BASE_URL}/browse/new-releases", headers=headers, params=params)
    response.raise_for_status()
    return response.json()["albums"]["items"]

def get_album_tracks(access_token: str, album_id: str) -> List[Dict[str, Any]]:
    """Fetches all tracks for a given album and enriches them with album details."""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Get album details first to enrich the tracks
    album_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}"
    album_response = requests.get(album_url, headers=headers)
    album_response.raise_for_status()
    album_details = album_response.json()

    # Get tracks for the album
    tracks_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks"
    tracks_response = requests.get(tracks_url, headers=headers)
    tracks_response.raise_for_status()
    tracks = tracks_response.json()["items"]

    # Denormalize: Add album and popularity info to each track
    for track in tracks:
        track['album'] = {
            'id': album_details.get('id'),
            'name': album_details.get('name'),
            'release_date': album_details.get('release_date'),
            'album_type': album_details.get('album_type')
        }
        # The `/albums/{id}/tracks` endpoint doesn't return track popularity.
        # We will fetch this in a later, more advanced version. For now, we can get album popularity.
        track['popularity'] = album_details.get('popularity', 0)
        
    return tracks
```

## tests/__init__.py
```python

```


```

## scripts/apply_snowflake_ddl.py
```python
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

```

## snowflake_ddl/001_foundational_setup.sql
```
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
```

## snowflake_ddl/002_create_tracks_staging_table.sql
```
-- 002_create_tracks_staging_table.sql
-- Description: Creates the staging table for raw track data from S3.

USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW_LANDING;

CREATE TABLE IF NOT EXISTS TRACKS_STAGING (
    RAW_TRACK_DATA      VARIANT,          -- Column to hold the entire JSON object for a track.
    S3_FILE_PATH        VARCHAR(1024),    -- The full S3 path of the file this record came from.
    LOADED_AT           TIMESTAMP_NTZ     -- Timestamp when the record was loaded via COPY INTO.
);
```

## snowflake_ddl/003_create_track_dimension_table.sql
```
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
```

## src/__init__.py
```python

```

## src/auth.py
```python
import requests
from requests.auth import HTTPBasicAuth

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"

def get_spotify_access_token(client_id: str, client_secret: str) -> str:
    """Obtains an access token from the Spotify API."""
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    
    response = requests.post(SPOTIFY_AUTH_URL, auth=auth, data=data)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    
    token_info = response.json()
    return token_info["access_token"]
```

## src/config.py
```python
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
```

## src/lambda_handler.py
```python
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
```

## src/s3_manager.py
```python
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
```

## src/snowflake_manager.py
```python
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
```

## src/spotify_client.py
```python
import requests
from typing import List, Dict, Any

SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

def get_new_releases(access_token: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Fetches newly released albums from Spotify."""
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": limit}
    response = requests.get(f"{SPOTIFY_API_BASE_URL}/browse/new-releases", headers=headers, params=params)
    response.raise_for_status()
    return response.json()["albums"]["items"]

def get_album_tracks(access_token: str, album_id: str) -> List[Dict[str, Any]]:
    """Fetches all tracks for a given album and enriches them with album details."""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Get album details first to enrich the tracks
    album_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}"
    album_response = requests.get(album_url, headers=headers)
    album_response.raise_for_status()
    album_details = album_response.json()

    # Get tracks for the album
    tracks_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks"
    tracks_response = requests.get(tracks_url, headers=headers)
    tracks_response.raise_for_status()
    tracks = tracks_response.json()["items"]

    # Denormalize: Add album and popularity info to each track
    for track in tracks:
        track['album'] = {
            'id': album_details.get('id'),
            'name': album_details.get('name'),
            'release_date': album_details.get('release_date'),
            'album_type': album_details.get('album_type')
        }
        # The `/albums/{id}/tracks` endpoint doesn't return track popularity.
        # We will fetch this in a later, more advanced version. For now, we can get album popularity.
        track['popularity'] = album_details.get('popularity', 0)
        
    return tracks
```

## tests/__init__.py
```python

```


```

## scripts/apply_snowflake_ddl.py
```python
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

```

## snowflake_ddl/001_foundational_setup.sql
```
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
```

## snowflake_ddl/002_create_tracks_staging_table.sql
```
-- 002_create_tracks_staging_table.sql
-- Description: Creates the staging table for raw track data from S3.

USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW_LANDING;

CREATE TABLE IF NOT EXISTS TRACKS_STAGING (
    RAW_TRACK_DATA      VARIANT,          -- Column to hold the entire JSON object for a track.
    S3_FILE_PATH        VARCHAR(1024),    -- The full S3 path of the file this record came from.
    LOADED_AT           TIMESTAMP_NTZ     -- Timestamp when the record was loaded via COPY INTO.
);
```

## snowflake_ddl/003_create_track_dimension_table.sql
```
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
```

## src/__init__.py
```python

```

## src/auth.py
```python
import requests
from requests.auth import HTTPBasicAuth

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"

def get_spotify_access_token(client_id: str, client_secret: str) -> str:
    """Obtains an access token from the Spotify API."""
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    
    response = requests.post(SPOTIFY_AUTH_URL, auth=auth, data=data)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    
    token_info = response.json()
    return token_info["access_token"]
```

## src/config.py
```python
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
```

## src/lambda_handler.py
```python
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
```

## src/s3_manager.py
```python
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
```

## src/snowflake_manager.py
```python
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
```

## src/spotify_client.py
```python
import requests
from typing import List, Dict, Any

SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

def get_new_releases(access_token: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Fetches newly released albums from Spotify."""
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": limit}
    response = requests.get(f"{SPOTIFY_API_BASE_URL}/browse/new-releases", headers=headers, params=params)
    response.raise_for_status()
    return response.json()["albums"]["items"]

def get_album_tracks(access_token: str, album_id: str) -> List[Dict[str, Any]]:
    """Fetches all tracks for a given album and enriches them with album details."""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Get album details first to enrich the tracks
    album_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}"
    album_response = requests.get(album_url, headers=headers)
    album_response.raise_for_status()
    album_details = album_response.json()

    # Get tracks for the album
    tracks_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks"
    tracks_response = requests.get(tracks_url, headers=headers)
    tracks_response.raise_for_status()
    tracks = tracks_response.json()["items"]

    # Denormalize: Add album and popularity info to each track
    for track in tracks:
        track['album'] = {
            'id': album_details.get('id'),
            'name': album_details.get('name'),
            'release_date': album_details.get('release_date'),
            'album_type': album_details.get('album_type')
        }
        # The `/albums/{id}/tracks` endpoint doesn't return track popularity.
        # We will fetch this in a later, more advanced version. For now, we can get album popularity.
        track['popularity'] = album_details.get('popularity', 0)
        
    return tracks
```

## tests/__init__.py
```python

```

