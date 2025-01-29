import json
import boto3
import pydicom
import numpy as np
import os
from supabase import create_client, Client
from uuid import uuid4
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
SNS_TOPIC_ARN = os.getenv('PENDING_TOPIC_ARN')
ERROR_TOPIC_ARN = os.getenv('ERROR_TOPIC_ARN')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# Initialize AWS SNS client
sns_client = boto3.client('sns')
# Global conversion_id
conversion_id = str(uuid4())

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

# Helper Functions
def update_files_status(file_id, status):
    """Update the status of a file in the files table."""
    data = {
        'status': status,
        'updated_at': datetime.utcnow().isoformat()
    }
    supabase.table('files').update(data).eq('file_id', file_id).execute()

def update_status_history(status_id, status, details):
    """Update the status history table."""
    data = {
        'status': status,
        'details': details,
    }
    supabase.table('status_history').update(data).eq('status_id', status_id).execute()

def insert_file_conversion(conversion_id, file_id, status, raw_file_path, presigned_raw_url, presigned_json_url):
    """Insert a new entry in the file_conversion table."""
    data = {
        'conversion_id': conversion_id,
        'file_id': file_id,
        'raw_file_path': raw_file_path,
        'status': status,
        'created_at': datetime.utcnow().isoformat(),
        'pre_signed_raw': presigned_raw_url,
        'pre_signed_json': presigned_json_url
    }
    supabase.table('file_conversion').insert(data).execute()

def generate_presigned_url(bucket_name, object_key, expiration=3600):
    """Generate a presigned URL for accessing an S3 object."""
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
    except Exception as e:
        print(f"Error generating presigned URL for {object_key}: {e}")
        return None
def lambda_handler(event, context):
    print("### Lambda Invoked ###")
    
    try:
        # Loop through all records in the SNS event
        for record in event['Records']:
            print("Processing SNS Record...")
            sns_message = record['Sns']['Message']
            print(f"Raw SNS Message: {sns_message}")
            
            # Parse the SNS message
            message_data = json.loads(sns_message)
            print(f"Parsed SNS Message: {message_data}")

            # Extract required values
            company_id = message_data.get('company_id')
            user_id = message_data.get('user_id')
            status_id = message_data.get('status_id')
            upload_id = message_data.get('upload_id')
            print(f"Extracted Values - Company ID: {company_id}, User ID: {user_id}, Status ID: {status_id}, Upload ID: {upload_id}")

            # Validate required fields
            if not (company_id and user_id and upload_id):
                send_error_notification(
                        error_type="VALIDATION_ERROR",
                        error_title="Missing Required Fields",
                        error_description=f"Missing required keys in SNS message for upload_id: {upload_id}",
                        user_id=user_id
                    )
                raise ValueError("Missing required keys in SNS message.")
            
            # Define S3 paths
            bucket = 'naviget-chandu-test'
            folder_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/"
            folder_key = folder_key.rstrip('/') + '/'
            print(f"S3 Folder Key: {folder_key}")

            # List objects in S3
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_key)
            print(f"S3 Response: {response}")

            if 'Contents' not in response:
                print(f"No files found in S3 folder: {folder_key}")
                send_error_notification(
                        error_type="S3_ERROR",
                        error_title="No Files Found",
                        error_description=f"No files found in folder: {folder_key}",
                        user_id=user_id
                    )
                return {
                    'statusCode': 400,
                    'body': json.dumps(f'No files found in the folder: {folder_key}')
                }

            combined_pixel_data = []
            combined_metadata = {}
            valid_shape = (512, 512)
            print("Starting to process files from S3...")

            for file_obj in response['Contents']:
                key = file_obj['Key']
                print(f"Found File: {key}")
                
                if key.endswith('/'):
                    print(f"Skipping folder: {key}")
                    continue

                tmp_input_path = f'/tmp/{os.path.basename(key)}'
                print(f"Downloading file to temporary path: {tmp_input_path}")
                s3_client.download_file(bucket, key, tmp_input_path)

                try:
                    # Process DICOM file
                    dicom_dataset = pydicom.dcmread(tmp_input_path, force=True)
                    if hasattr(dicom_dataset, "PixelData"):
                        pixel_array = dicom_dataset.pixel_array
                        print(f"Pixel Data Shape: {pixel_array.shape}")
                        
                        if pixel_array.shape == valid_shape:
                            combined_pixel_data.append(pixel_array)
                            print(f"Valid pixel data added from file: {key}")
                        else:
                            print(f"Skipping file with invalid pixel shape: {pixel_array.shape}")
                    
                    # Extract metadata
                    for tag in dicom_dataset.dir():
                        if tag != "PixelData" and tag not in combined_metadata:
                            combined_metadata[tag] = str(dicom_dataset.get(tag, "Unknown"))
                            print(f"Metadata tag added: {tag}")
                finally:
                    os.remove(tmp_input_path)
                    print(f"Temporary file removed: {tmp_input_path}")

            if combined_pixel_data:
                try:
                    combined_pixel_data = np.concatenate(combined_pixel_data, axis=0)
                    print(f"Combined Pixel Data Shape: {combined_pixel_data.shape}")
                except Exception as e:
                    print(f"Error combining pixel data: {e}")
                    combined_pixel_data = None

            # Define output file paths
            parent_folder = folder_key.rsplit('/', 3)[0]
            base_output_key = f"{parent_folder}/{upload_id}/temp/{upload_id}"
            raw_output_key = f"{base_output_key}.raw"
            metadata_output_key = f"{base_output_key}-settings.json"
            print(f"Output Keys - RAW: {raw_output_key}, Metadata: {metadata_output_key}")

            tmp_raw_path = f"/tmp/{upload_id}.raw"
            tmp_metadata_path = f"/tmp/{upload_id}-settings.json"

            try:
                # Write RAW and metadata files locally
                with open(tmp_raw_path, 'wb') as raw_file:
                    if combined_pixel_data is not None:
                        combined_pixel_data.tofile(raw_file)
                        print(f"Raw file written locally: {tmp_raw_path}")

                with open(tmp_metadata_path, 'w') as metadata_file:
                    json.dump(combined_metadata, metadata_file, indent=4)
                    print(f"Metadata file written locally: {tmp_metadata_path}")

                # Upload files to S3
                s3_client.upload_file(tmp_raw_path, bucket, raw_output_key)
                print(f"Uploaded RAW file to S3: {raw_output_key}")

                s3_client.upload_file(tmp_metadata_path, bucket, metadata_output_key)
                print(f"Uploaded metadata file to S3: {metadata_output_key}")

            finally:
                os.remove(tmp_raw_path)
                os.remove(tmp_metadata_path)
                print(f"Temporary files cleaned up.")

            # Generate presigned URLs
            raw_file_url = generate_presigned_url(bucket, raw_output_key)
            json_file_url = generate_presigned_url(bucket, metadata_output_key)
            print(f"Presigned URLs - RAW: {raw_file_url}, Metadata: {json_file_url}")

            # Update database and send SNS notification
            try:
                update_files_status(upload_id, 'raw_file_generated')
                update_status_history(status_id, 'raw_file_generated', 'Raw File has been extracted.')
                insert_file_conversion(conversion_id, upload_id, 'pending', raw_output_key, raw_file_url, json_file_url)
            except Exception as e:
                send_error_notification(
                    error_type="DATABASE_ERROR",
                    error_title="Database Update Failed",
                    error_description=f"Failed to update database for upload_id {upload_id}: {str(e)}",
                    user_id=user_id
                    )
                raise        

            sns_message = {
                "company_id": company_id,
                "user_id": user_id,
                "upload_id": upload_id,
                "status_id": status_id,
                "conversion_id": conversion_id,
                "status": "Success",
                "message": "Successfully extracted raw file",
                "timestamp": datetime.utcnow().isoformat()
            }
            print(f"Publishing SNS Message: {sns_message}")

            sns_response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(sns_message),
                Subject="File Extraction Status"
            )
            print(f"Published SNS Message. Message ID: {sns_response['MessageId']}")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed all valid files in {folder_key} into one RAW and metadata JSON file")
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
