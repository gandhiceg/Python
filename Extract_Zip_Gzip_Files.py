import boto3
import gzip
import zipfile
import tarfile
import io
from io import BytesIO
import re
import os

def extract_gzip_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern, destination_bucket_name, destination_prefix):
    
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
        print(key)
        source_key=key
        #destination_key=destination_prefix+source_key.replace(".gz","")
        
        try:

            response = s3_client.get_object(Bucket=source_bucket_name, Key=source_key)
            gzipped_content = response['Body'].read()            
                    
            with gzip.GzipFile(fileobj=BytesIO(gzipped_content), mode='rb') as gz_file:
                decompressed_content = gz_file.read()
                
                start_index = source_key.rfind('/')+1
                end_index = start_index + len(source_key)
                file_name = source_key[start_index:end_index]
                #print(file_name)
                
                dest_key = f"{destination_prefix}/{file_name.replace(".gz","")}" if destination_prefix else file_name
                        
                s3_client.put_object(Bucket=destination_bucket_name, Key=dest_key, Body=decompressed_content)

                                        
                print(f"Uploaded {file_name} to s3://{destination_bucket_name}/{dest_key}")

        except Exception as e:
            print(f"Error processing S3 file: {e}")
                       
def extract_zip_from_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern, destination_bucket_name, destination_prefix):
    
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
        #destination_key=destination_prefix+source_key.replace(".zip","")
        
        try:

    
            zip_object = s3_client.get_object(Bucket=source_bucket_name, Key=source_key)
            buffer = BytesIO(zip_object['Body'].read())

        
            with zipfile.ZipFile(buffer, 'r') as zf:
            
                for file_info in zf.infolist():
                
                    if file_info.is_dir():
                        continue

                
                
                    dest_key = f"{destination_prefix}/{file_info.filename.replace(".zip","")}" if destination_prefix else file_info.filename
                
                
                    file_content = zf.read(file_info.filename)

                
                    s3_client.put_object(
                    Bucket=destination_bucket_name,
                    Key=dest_key,
                    Body=file_content
                    )
                    print(f"Uploaded {file_info.filename} to s3://{destination_bucket_name}/{dest_key}")

        except Exception as e:
            print(f"Error extracting or uploading files: {e}")

def extract_tar_from_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern, destination_bucket_name, destination_prefix):
    
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
        print(key)
        source_key=key

    try:
        
        print(f"Downloading {key} from s3://{source_bucket_name}")
        response = s3_client.get_object(Bucket=source_bucket_name, Key=key)
        tar_data = response['Body'].read()

        
        tar_file_object = io.BytesIO(tar_data)
        mode = 'r:gz' if key.endswith('.gz') else 'r'

        with tarfile.open(fileobj=tar_file_object, mode=mode) as tar_ref:
            
            for member in tar_ref.getmembers():
                if member.isfile():  # Only process files
                    extracted_file_name = os.path.basename(member.name)
                    s3_destination_key = os.path.join(destination_prefix, extracted_file_name).replace("\\", "/")

                    print(f"Uploading {extracted_file_name} to s3://{destination_bucket_name}/{s3_destination_key}")
                    extracted_file_content = tar_ref.extractfile(member).read()
                    s3_client.put_object(
                        Bucket=destination_bucket_name,
                        Key=s3_destination_key,
                        Body=extracted_file_content
                    )
        print("Tar extraction and upload complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        

def merge_s3_files(source_bucket_name, source_prefix, file_name_pattern , file_name, destination_bucket_name, destination_prefix):
    
    s3 = boto3.client('s3')
    merged_content = io.BytesIO()

    try:
        
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)
        regex = re.compile(file_name_pattern)

        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    if re.search(regex, obj['Key']):
                        print(f"Found matching file: {obj['Key']}")
                        # Get object content
                        response = s3.get_object(Bucket=source_bucket_name, Key=obj['Key'])
                        content = response['Body'].read()
                        merged_content.write(content)

        
        merged_content.seek(0)
        dest_key = f"{destination_prefix}/{file_name}" if destination_prefix else file_name
        s3.put_object(Bucket=destination_bucket_name, Key=dest_key, Body=merged_content.getvalue())
        print(f"Successfully merged files to s3://{destination_bucket_name}/{dest_key}")

    except Exception as e:
        print(f"An error occurred: {e}")


source_bucket_name = '<s3_source_bucket_name>'
source_prefix = '<source_prefix>' # Optional prefix to narrow down the search
file_name_pattern = '.*\\.tsv\\.gz$' # Regex pattern to match all .csv.gz files in the prefix
destination_bucket_name = '<s3_target_bucket_name>'
destination_prefix = '<destination_prefix>'
extract_gzip_s3_to_s3(source_bucket_name, source_prefix, file_name_pattern ,destination_bucket_name, destination_prefix)
