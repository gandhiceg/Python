import boto3
import re
from datetime import date, timedelta

def copy_files_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern, destination_bucket_name, destination_prefix):

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)

    regex = re.compile(file_name_pattern)
    matching_keys = []

    for page in page_iterator:
        if 'Contents' in page:
            for item in page['Contents']:
                if re.match(regex, item['Key']):
                    matching_keys.append(item['Key'])

    print(f"Found {len(matching_keys)} matching files:")
    for key in matching_keys:
        #print(key)
        source_key=key
        start_index = source_key.rfind('/')+1
        end_index = start_index + len(source_key)
        file_name = source_key[start_index:end_index]
        #destination_key=destination_prefix+source_key.replace(".gz","")
        
        try:

            #destination_key = source_key
            destination_key = f"{destination_prefix}/{file_name}" if destination_prefix else file_name            
            
            s3_client.copy_object(
                Bucket=destination_bucket_name,
                CopySource={'Bucket': source_bucket_name, 'Key': source_key},
                Key=destination_key
                )
            print(f"Copied '{source_key}' to '{destination_key}'.")
        
        except Exception as e:
            print(f"Error during search and copy: {e}")
    

source_bucket_name = '<s3_source_bucket_name>'
destination_bucket_name = '<s3_target_bucket_name>'
source_prefix = '<source_prefix>'
destination_prefix = '<destination_prefix>'
today = date.today()
yesterday = today - timedelta(days=1)
file_name_pattern = f'.*{yesterday}.*'

copy_files_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern, destination_bucket_name, destination_prefix)
