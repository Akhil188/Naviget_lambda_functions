import io
import zipfile
import os
import boto3
from clients import s3_client
from notifications import send_error_notification
from pydicom.filebase import DicomBytesIO
from pydicom.filereader import dcmread
import pydicom
from pydicom.filebase import DicomFile
from pydicom.pixel_data_handlers.util import apply_modality_lut
import shutil

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks

s3_client = boto3.client("s3")
import os
import zipfile

def process_zip_file_streaming(bucket_name: str, object_key: str, extracted_prefix: str, user_id: str, company_id: str, upload_id: str) -> bool:
    """Process the ZIP file or an unzipped folder, handling nested ZIPs if found"""
    
    print("bucket_name:", bucket_name)
    print("object_key:", object_key)
    print("extracted_prefix:", extracted_prefix)

    try:
        print(f"Starting processing for object: {object_key}")
        extracted_folder = "/tmp/extracted_dicom"
        os.makedirs(extracted_folder, exist_ok=True)
        
        # If the object is a folder, process it
        if object_key.endswith('/'):
            return process_folder(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id)
        
        # If it's a ZIP file, extract it
        if object_key.endswith('.zip'):
            success = process_zip(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id)
            
            # Check if extracted files contain any more ZIPs
            extract_nested_zips(extracted_folder)
            
            # Cleanup any remaining ZIPs
            remove_leftover_zip_files(extracted_folder)
            
            return success
        
    except Exception as e:
        error_description = f"Failed to process file {object_key} in bucket {bucket_name}. Error: {str(e)}"
        print(f"Error in process_zip_file_streaming: {error_description}")
        send_error_notification("FILE_PROCESSING_ERROR", "File Processing Failed", error_description, user_id)
        return False

def extract_nested_zips(folder: str):
    """Extracts any ZIP files found within the extracted folder"""
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                extract_path = os.path.join(root, file.replace(".zip", ""))  # Create a subfolder for extraction
                
                print(f"Extracting nested ZIP: {zip_path} to {extract_path}")
                os.makedirs(extract_path, exist_ok=True)
                
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_path)
                    os.remove(zip_path)  # Delete the ZIP after extraction
                except Exception as e:
                    print(f"Error extracting nested ZIP {zip_path}: {e}")

def remove_leftover_zip_files(folder: str):
    """Removes any ZIP files left in the extracted folder after processing"""
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                print(f"Removing leftover ZIP: {zip_path}")
                os.remove(zip_path)


def process_folder(bucket_name, object_key, extracted_folder, user_id, company_id, upload_id):
    """Processes a folder from S3 by downloading its contents and re-uploading."""
    print(f"Detected folder: {object_key}")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=object_key):
            for obj in page.get('Contents', []):
                file_key = obj['Key']
                relative_path = os.path.relpath(file_key, object_key)
                download_path = os.path.join(extracted_folder, relative_path)
                try:
                    os.makedirs(os.path.dirname(download_path), exist_ok=True)
                    s3_client.download_file(bucket_name, file_key, download_path)
                    print(f"Downloaded file from folder: {file_key} to {download_path}")
                    process_all_compressed(extracted_folder)
                    s3_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}/{relative_path}"
                    s3_client.upload_file(download_path, bucket_name, s3_key)
                    print(f"Uploaded file to S3: {s3_key}")
                except Exception as e:
                    print(f"Unexpected error while processing {file_key}: {e}")
    except Exception as e:
        print(f"Unexpected error while processing folder {object_key}: {e}")
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
            print("zip_path",zip_path)
            print("fextracted_folder",extracted_folder)
            process_all_compressed(extracted_folder)

            for root, dirs, files in os.walk(extracted_folder):
                # Print the current directory
                print(f"Folder: {root}")
                
                # Print all files in the current directory
                for file in files:
                    print(f"  File: {file}")

            for root, dirs, files in os.walk(extracted_folder):
                for filename in files:
                    #print("filename",filename)
                    file_path = os.path.join(root, filename)
                    # Construct the S3 key based on the folder structure
                    relative_path = os.path.relpath(file_path, extracted_folder)
                    s3_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}/{relative_path}"
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    #print(f"Uploaded extracted file to S3: {s3_key}")

            return True  # Return True if extraction was successful
            
        except Exception as e:
            raise Exception(f"Failed during ZIP extraction: {str(e)}")

def process_all_compressed(directory):
    """Recursively checks and extracts all compressed files in a directory."""
    print("Directory1",directory)
    if directory.endswith("_decompressed"):
        print(f"Skipping decompressed folder: {directory}")
        return
    # Define the decompressed directory name
    for root, _, files in os.walk(directory):
        print("root",root)
        print("files",files)
        if root.endswith("_decompressed"):
            continue
        decompressed_dir = f"{root}_decompressed"
        os.makedirs(decompressed_dir, exist_ok=True)
        for file in files:
            # Check if the file is a DICOM file (optional, based on file extension)
            dcm_file_path = os.path.join(root, file)              
            try:
                # Read the DICOM file
                dicom_data = pydicom.dcmread(dcm_file_path)
                
                # Decompress the DICOM file
                dicom_data.decompress()
                
                decompressed_file_path = os.path.join(decompressed_dir, file)
                dicom_data.save_as(decompressed_file_path)
                print(f"Decompressed: {dcm_file_path} → {decompressed_file_path}")
            except Exception as e:
                print(f"Error processing {dcm_file_path}: {e}")
        if os.path.exists(decompressed_dir) and os.listdir(decompressed_dir) and os.path.isdir(root):
            print(f"Removing old directory: {root}")
            shutil.rmtree(root)
            print(f"Old directory removed: {root}")
        
