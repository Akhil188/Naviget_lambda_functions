import io
import zipfile
import os
import boto3
from clients import s3_client
from notifications import send_error_notification

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks

s3_client = boto3.client("s3")

def process_zip_file_streaming(bucket_name: str, object_key: str, extracted_prefix: str, user_id: str) -> bool:
    """Process the ZIP file using streaming to minimize memory usage"""
    try:
        print(f"Starting ZIP processing for object: {object_key}")
        
        # Get object size
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = response['ContentLength']
            print(f"File size: {file_size} bytes")
        except Exception as e:
            raise Exception(f"Failed to get object size: {str(e)}")
        
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

            # Create the extracted folder if it doesn't exist
            extracted_folder = "/tmp/extracted_dicom"
            os.makedirs(extracted_folder, exist_ok=True)

            # Extract zip contents
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_folder)  # Extract to the specified folder

            # Log the contents of the extracted folder
            print(f"Extracted files: {os.listdir(extracted_folder)}")
            
            return True  # Return True if extraction was successful
                
        except Exception as e:
            raise Exception(f"Failed during ZIP extraction: {str(e)}")
        
    except Exception as e:
        error_description = f"Failed to process ZIP file {object_key} in bucket {bucket_name}. Error: {str(e)}"
        print(f"Error in process_zip_file_streaming: {error_description}")
        send_error_notification(
            error_type="ZIP_PROCESSING_ERROR",
            error_title="ZIP File Processing Failed",
            error_description=error_description,
            user_id=user_id
        )
        return False 