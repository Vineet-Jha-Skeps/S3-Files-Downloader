import pandas as pd
import boto3
import os
from botocore.exceptions import ClientError

# Credentials
AWS_ACCESS_KEY_ID= ""
AWS_SECRET_ACCESS_KEY= ""
AWS_SESSION_TOKEN= ""

# S3 connector
s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key= AWS_SECRET_ACCESS_KEY, aws_session_token = AWS_SESSION_TOKEN)

# Paginator to handle pagination
paginator = s3.get_paginator('list_objects_v2') 

# S3 bucket and folder to read from
BUCKET_NAME = "ssp-dps-prod"
S3_FOLDER = "Amerifirst/originations/hil_transaction/"

# Folder to download data in
LOCAL_FOLDER = os.path.join(os.getcwd(), 'Data', 'Transactions_Files')

# Create directory if it doesn't exist
try:
    os.makedirs(LOCAL_FOLDER, exist_ok=True)
except Exception as e:
    print(f"Error creating directory: {e}")

page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_FOLDER)

# Iterating through the page to list all objects in that bucket folder and download it
for page in page_iterator:
    files = page.get("Contents")
    for file in files:
        try:
            s3_file_path = file.get("Key")
            file_name = os.path.basename(s3_file_path)
            local_file_path = os.path.join(LOCAL_FOLDER, file_name)

            if "hil_transaction_" in local_file_path and ".txt" in file_name:
                print(f"Downloading {file_name}")
                s3.download_file(BUCKET_NAME, s3_file_path, local_file_path)

        except Exception as e:
            print(f"Error: {e}")