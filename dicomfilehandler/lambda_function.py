import os
from supabase import create_client, Client
import boto3
import zipfile
import io
from uuid import uuid4
from datetime import datetime
import json

# Environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
SNS_TOPIC_ARN = os.getenv('PENDING_TOPIC_ARN')
ERROR_TOPIC_ARN = os.getenv('ERROR_TOPIC_ARN')

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Constants
CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks

def send_error_notification(error_type: str, error_title: str, error_description: str, user_id: str) -> None:
    """Send error notification via SNS"""
    try:
        message = {
            'error_type': error_type,
            'error_title': error_title,
            'error_description': error_description,
            'user_id': user_id
        }
        
        sns_client.publish(
            TopicArn=ERROR_TOPIC_ARN,
            Message=json.dumps(message)
        )
        print(f"Error notification sent: {error_type} - {error_title}")
        
    except Exception as e:
        print(f"Failed to send error notification: {str(e)}")

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
            with zipfile.ZipFile(zip_buffer) as zip_file:
                file_count = 0
                for file_info in zip_file.infolist():
                    if file_info.file_size > 0:  # Skip directories
                        extracted_key = f"{extracted_prefix}/{file_info.filename}"
                        print(f"Processing file: {file_info.filename}")
                        
                        # Stream each file from zip to S3
                        with zip_file.open(file_info) as source_file:
                            s3_client.upload_fileobj(
                                source_file,
                                bucket_name,
                                extracted_key,
                                ExtraArgs={'ContentType': 'application/octet-stream'}
                            )
                            file_count += 1
                            print(f"Extracted: {extracted_key}")
                
                print(f"Successfully extracted {file_count} files")
                return True
                
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

def update_file_record(upload_id: str, status: str) -> bool:
    """Update the file status in Supabase"""
    try:
        data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        supabase.table('files').update(data).eq('file_id', upload_id).execute()
        return True
            
    except Exception as e:
        error_description = f"Failed to update file record (ID: {upload_id}) with status: {status}. Error: {str(e)}"
        print(error_description)  # Log the error
        return False

def insert_status_history(file_id: str, user_id: str, status: str, status_id: str, details: str = None) -> bool:
    """Insert status history record"""
    try:
        data = {
            'status_id': status_id,
            'file_id': file_id,
            'status': status,
            'details': details,
            'uploaded_by': user_id,
            'created_at': datetime.utcnow().isoformat()
        }
        
        supabase.table('status_history').insert(data).execute()
        return True
            
    except Exception as e:
        error_description = f"Failed to insert status history for file {file_id}. Error: {str(e)}"
        print(error_description)  # Log the error
        return False

def lambda_handler(event, context):
    """Main Lambda handler with memory optimization"""
    try:
        print(f"Processing event: {json.dumps(event)}")

        record = event['Records'][0]['s3']
        bucket_name = record['bucket']['name']
        object_key = record['object']['key']

        # Parse object key
        parts = object_key.split('/')
        if len(parts) < 6:
            raise ValueError(f"Invalid object key structure: {object_key}")

        company_id = parts[0]
        user_id = parts[1]
        upload_id = parts[3]
        status_id = str(uuid4())

        # Validate file
        if not object_key.endswith(f"{upload_id}.zip") or parts[4] != 'input':
            return {
                "statusCode": 200,
                "body": "File skipped - not for processing"
            }

        # Process ZIP
        extracted_prefix = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}"
        if process_zip_file_streaming(bucket_name, object_key, extracted_prefix, user_id):
            # Success path
            if not update_file_record(upload_id, "extracted"):
                send_error_notification(
                    error_type="DATABASE_ERROR",
                    error_title="Status Update Failed",
                    error_description=f"Failed to update status to EXTRACTED for upload ID: {upload_id}",
                    user_id=user_id
                )

            if not insert_status_history(upload_id, user_id, "extracted", status_id,"Extraction complete"):
                send_error_notification(
                    error_type="DATABASE_ERROR",
                    error_title="Status History Update Failed",
                    error_description=f"Failed to insert EXTRACTED status history for upload ID: {upload_id}",
                    user_id=user_id
                )

            # Notify next step
            if SNS_TOPIC_ARN:
                message_content = {
                    "company_id": company_id,
                    "user_id": user_id,
                    "status_id": status_id,  # Generating a new UUID for status_id
                    "upload_id": upload_id,
                    "status": "Success",
                    "message": f"Successfully extracted and uploaded files from {bucket_name}/{extracted_prefix}",
                    "timestamp": datetime.utcnow().isoformat()
                }

                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=json.dumps(message_content)
                )
                print(f"SNS message published successfully: {json.dumps(message_content)}")

            return {
                    "statusCode": 200,
                    "body": "Processing completed successfully"
                }

        else:
            # Failure path
            print("ZIP processing failed")
            return {
                "statusCode": 500,
                "body": "Processing failed"
            }

    except Exception as e:
        error_description = f"Unexpected error during file processing: {str(e)}"
        send_error_notification(
            error_type="GENERAL_ERROR",
            error_title="Unexpected Processing Error",
            error_description=error_description,
            user_id=user_id if 'user_id' in locals() else 'unknown'
        )

        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
