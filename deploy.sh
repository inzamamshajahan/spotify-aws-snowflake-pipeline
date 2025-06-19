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