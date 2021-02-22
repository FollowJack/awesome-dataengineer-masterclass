import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import json
import os


def store_to_file_as_json(data, path):
    with open(path, 'w') as outfile:
        json.dump(data, outfile)


def store_to_file_as_csv(df, path_target):
    df.to_csv(path_target)


def upload_to_aws(local_file, bucket, s3_file):
    load_dotenv()

    ACCESS_KEY = os.getenv('ID_AWS_ACCESS_API')
    SECRET_KEY = os.getenv('KEY_AWS_ACCESS')

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
