import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import json
import os
from datetime import datetime


def store_to_file_as_json(data, path):
    before = datetime.now()
    with open(path, 'w') as outfile:
        json.dump(data, outfile)

    duration = (datetime.now() - before).total_seconds()
    print(f"SUCCESS: JSON stored to {path} in {duration} seconds.")


def store_to_file_as_csv(df, path_target):
    before = datetime.now()
    df.to_csv(path_target)
    duration = (datetime.now() - before).total_seconds()
    print(f"SUCCESS: CSV stored to {path_target} in {duration} seconds.")


def upload_to_aws(local_file, bucket, s3_file):
    load_dotenv()

    ACCESS_KEY = os.getenv('ID_AWS_ACCESS_API')
    SECRET_KEY = os.getenv('KEY_AWS_ACCESS')

    print("INFO: Connect to S3 bucket")
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        print("INFO: Upload file to S3 bucket")
        before = datetime.now()
        s3.upload_file(local_file, bucket, s3_file)
        duration = (datetime.now() - before).total_seconds()
        print(f"SUCCESS: Finished upload to S3 in {duration} seconds.")
        return True
    except FileNotFoundError:
        print("ERROR: The file was not found")
        return False
    except NoCredentialsError:
        print("ERROR: Credentials not available")
        return False
