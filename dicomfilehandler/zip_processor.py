import io
import zipfile
import os
import boto3
from clients import s3_client
from notifications import send_error_notification

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks

s3_client = boto3.client("s3")

def process_zip_file_streaming(bucket_name: str, object_key: str, extracted_prefix: str, user_id: str, company_id: str, upload_id: str) -> bool:
    """Process the ZIP file or unzipped folder using streaming to minimize memory usage"""
    print("bucket_name",bucket_name)
    print("object_key",object_key)
    print("extracted_prefix",extracted_prefix)
    try:
        print(f"Starting processing for object: {object_key}")
        extracted_folder = "/tmp/extracted_dicom"
        os.makedirs(extracted_folder, exist_ok=True)
        
        # Check if object is a folder or a file
        if object_key.endswith('/'):
            return process_folder(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id)
        
        if object_key.endswith('.zip'):
            return process_zip(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id)
         
    except Exception as e:
        error_description = f"Failed to process file {object_key} in bucket {bucket_name}. Error: {str(e)}"
        print(f"Error in process_zip_file_streaming: {error_description}")
        send_error_notification("FILE_PROCESSING_ERROR", "File Processing Failed", error_description, user_id)
        return False

def process_folder(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id):
    """Processes a folder from S3 by downloading its contents and re-uploading."""
    print(f"Detected folder: {object_key}")
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=object_key):
        for obj in page.get('Contents', []):
            file_key = obj['Key']
            relative_path = os.path.relpath(file_key, object_key)
            download_path = os.path.join(extracted_folder, relative_path)
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            s3_client.download_file(bucket_name, file_key, download_path)
            print(f"Downloaded file from folder: {file_key} to {download_path}")
            process_all_compressed(download_path, extracted_folder)
            s3_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}/{relative_path}"
            s3_client.upload_file(download_path, bucket_name, s3_key)
            print(f"Uploaded file to S3: {s3_key}")
    return True

def process_zip(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        file_size = response['ContentLength']
        print(f"File size: {file_size} bytes")
    except Exception as e:
        raise Exception(f"Failed to get object size: {str(e)}")
    
    # Check if the object is a ZIP file
    if object_key.endswith('.zip'):
        # Stream the zip file in chunks
        zip_buffer = io.BytesIO()
        position = 0
        
        print("Starting chunk download")
        while position < file_size:
            chunk_end = min(position + CHUNK_SIZE, file_size)
            try:
                chunk = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Range=f'bytes={position}-{chunk_end-1}'
                )['Body'].read()
                
                zip_buffer.write(chunk)
                position = chunk_end
                print(f"Downloaded bytes: {position}/{file_size}")
                
            except Exception as e:
                raise Exception(f"Failed to download chunk at position {position}: {str(e)}")

        zip_buffer.seek(0)
        print("Download complete, starting extraction")
        
        # Process ZIP contents
        try:
            zip_path = f"/tmp/{os.path.basename(object_key)}"
            # Download ZIP from S3
            s3_client.download_file(bucket_name, object_key, zip_path)

            # Extract zip contents
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_folder)  # Extract to the specified folder

            # Log the contents of the extracted folder
            print(f"Extracted files: {os.listdir(extracted_folder)}")
            print("-----------------------------------")
            print("extracted_folder",extracted_folder)
            # After extraction, upload extracted files to S3
            for root, dirs, files in os.walk(extracted_folder):
                for filename in files:
                    print("filename",filename)
                    file_path = os.path.join(root, filename)
                    # Construct the S3 key based on the folder structure
                    relative_path = os.path.relpath(file_path, extracted_folder)
                    #process_all_compressed(, extracted_folder)
                    s3_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}/{relative_path}"
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Uploaded extracted file to S3: {s3_key}")

            return True  # Return True if extraction was successful
            
        except Exception as e:
            raise Exception(f"Failed during ZIP extraction: {str(e)}")

def process_all_compressed(directory, extracted_folder):
    """Recursively checks and extracts all compressed files in a directory."""
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_folder)
                print(f"Extracted ZIP: {file_path}")
            elif file_path.endswith('.tar.gz') or file_path.endswith('.tgz'):
                with tarfile.open(file_path, 'r:gz') as tar_ref:
                    tar_ref.extractall(extracted_folder)
                print(f"Extracted TAR.GZ: {file_path}")
            elif file_path.endswith('.tar'):
                with tarfile.open(file_path, 'r:') as tar_ref:
                    tar_ref.extractall(extracted_folder)
                print(f"Extracted TAR: {file_path}")