import json
import os
import warnings
from uuid import uuid4
from datetime import datetime
import pydicom
import numpy as np
from supabase import create_client

from database import DatabaseHandler
from s3_handler import S3Handler
from sns_handler import SNSHandler
from dicom_processor import DicomProcessor

# Initialize clients
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
SNS_TOPIC_ARN = os.getenv('PENDING_TOPIC_ARN')
ERROR_TOPIC_ARN = os.getenv('ERROR_TOPIC_ARN')

supabase = create_client(SUPABASE_URL, SUPABASE_API_KEY)
db_handler = DatabaseHandler(supabase)
s3_handler = S3Handler()
sns_handler = SNSHandler(ERROR_TOPIC_ARN, SNS_TOPIC_ARN)
dicom_processor = DicomProcessor()

def is_dicom_file(key: str) -> bool:
    """Check if the file is likely a DICOM file based on extension or lack thereof"""
    lower_key = key.lower()
    non_dicom_extensions = {'.txt', '.py', '.raw', '.json', '.csv', '.md'}
    return not any(lower_key.endswith(ext) for ext in non_dicom_extensions)

def process_pixel_data(dicom_dataset, key: str) -> np.ndarray:
    """Process pixel data with proper handling of signed/unsigned conversion"""
    pixel_array = dicom_dataset.pixel_array
    
    # Check if we need to convert from unsigned to signed
    if hasattr(dicom_dataset, 'PixelRepresentation') and dicom_dataset.PixelRepresentation == 1:
        # Convert unsigned to signed if needed
        pixel_array = pixel_array.astype(np.int16)
        # If the data was unsigned but marked as signed, we need to handle negative values
        pixel_array[pixel_array > 32767] -= 65536
        print(f"Converted pixel data for {key} from unsigned to signed representation")
    
    return pixel_array

def lambda_handler(event, context):
    print("### Lambda Invoked ###")
    conversion_id = str(uuid4())
    
    try:
        # Process each SNS record
        for record in event['Records']:
            print("Processing SNS Record...")
            message_data = json.loads(record['Sns']['Message'])
            print(f"Parsed SNS Message: {message_data}")

            # Extract required values
            company_id = message_data.get('company_id')
            user_id = message_data.get('user_id')
            status_id = message_data.get('status_id')
            upload_id = message_data.get('upload_id')
            
            # Validate required fields
            if not all([company_id, user_id, upload_id]):
                sns_handler.send_error_notification(
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
            response = s3_handler.list_objects(bucket, folder_key)
            print(f"S3 Response: {response}")

            if 'Contents' not in response:
                print(f"No files found in S3 folder: {folder_key}")
                sns_handler.send_error_notification(
                    error_type="S3_ERROR",
                    error_title="No Files Found",
                    error_description=f"No files found in folder: {folder_key}",
                    user_id=user_id
                )
                return {
                    'statusCode': 400,
                    'body': json.dumps(f'No files found in the folder: {folder_key}')
                }

            # Process DICOM files
            combined_pixel_data = []
            combined_metadata = {}
            valid_shape = (512, 512)
            processed_files = 0
            warning_messages = []

            for file_obj in response['Contents']:
                key = file_obj['Key']
                if key.endswith('/') or not is_dicom_file(key):
                    print(f"Skipping non-DICOM file or directory: {key}")
                    continue

                tmp_input_path = f'/tmp/{os.path.basename(key)}'
                s3_handler.download_file(bucket, key, tmp_input_path)

                try:
                    # Suppress warnings during DICOM read
                    with warnings.catch_warnings(record=True) as w:
                        dicom_dataset = pydicom.dcmread(tmp_input_path, force=True)
                        if w:
                            warning_messages.extend([str(warn.message) for warn in w])
                    
                    if hasattr(dicom_dataset, "PixelData"):
                        try:
                            pixel_array = process_pixel_data(dicom_dataset, key)
                            current_shape = pixel_array.shape
                            
                            if current_shape == valid_shape:
                                combined_pixel_data.append(pixel_array)
                                processed_files += 1
                                print(f"Successfully processed file: {key}")
                            else:
                                msg = f"Skipping file with invalid shape: {key} - Shape: {current_shape}"
                                print(msg)
                                warning_messages.append(msg)
                        except Exception as e:
                            error_msg = f"Error processing pixel data from {os.path.basename(key)}: {str(e)}"
                            print(error_msg)
                            warning_messages.append(error_msg)
                    else:
                        print(f"No pixel data found in file: {key}")

                    file_metadata = dicom_processor.process_dicom_metadata(dicom_dataset)
                    for tag, value in file_metadata.items():
                        if tag not in combined_metadata:
                            combined_metadata[tag] = value
                except Exception as e:
                    print(f"Error processing DICOM file {key}: {str(e)}")
                    warning_messages.append(f"Failed to process {os.path.basename(key)}: {str(e)}")
                finally:
                    os.remove(tmp_input_path)

            if not combined_pixel_data:
                error_msg = "No valid DICOM files were processed successfully"
                print(error_msg)
                sns_handler.send_error_notification(
                    error_type="PROCESSING_ERROR",
                    error_title="No Valid DICOM Data",
                    error_description=error_msg,
                    user_id=user_id
                )
                return {
                    'statusCode': 400,
                    'body': json.dumps(error_msg)
                }

            # Process combined data
            try:
                combined_pixel_data = np.concatenate(combined_pixel_data, axis=0)
                print(f"Successfully combined {processed_files} files into shape {combined_pixel_data.shape}")
            except Exception as e:
                error_msg = f"Error combining pixel data: {str(e)}"
                print(error_msg)
                sns_handler.send_error_notification(
                    error_type="PROCESSING_ERROR",
                    error_title="Data Combination Failed",
                    error_description=error_msg,
                    user_id=user_id
                )
                return {
                    'statusCode': 500,
                    'body': json.dumps(error_msg)
                }

            # Define output paths
            parent_folder = folder_key.rsplit('/', 3)[0]
            base_output_key = f"{parent_folder}/{upload_id}/temp/{upload_id}"
            raw_output_key = f"{base_output_key}.raw"
            metadata_output_key = f"{base_output_key}-settings.json"

            # Save and upload files
            tmp_raw_path = f"/tmp/{upload_id}.raw"
            tmp_metadata_path = f"/tmp/{upload_id}-settings.json"

            try:
                # Write RAW file
                if combined_pixel_data is not None:
                    with open(tmp_raw_path, 'wb') as raw_file:
                        combined_pixel_data.tofile(raw_file)

                # Prepare and write metadata
                output_dimensions = {
                    "width": int(combined_pixel_data.shape[1]) if combined_pixel_data is not None else 0,
                    "height": int(combined_pixel_data.shape[0]) if combined_pixel_data is not None else 0,
                    "depth": int(combined_pixel_data.shape[2]) if combined_pixel_data is not None and len(combined_pixel_data.shape) > 2 else 1
                }

                metadata_json = {
                    "dicom_metadata": combined_metadata,
                    "output_dimensions": output_dimensions,
                    "voxel_scale": {"x": 1.0, "y": 1.0, "z": 1.0},
                    "processing_info": {
                        "files_processed": processed_files,
                        "warnings": warning_messages,
                        "processed_at": datetime.utcnow().isoformat()
                    }
                }

                with open(tmp_metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata_json, f, indent=4, ensure_ascii=False)

                # Upload to S3
                s3_handler.upload_file(tmp_raw_path, bucket, raw_output_key)
                s3_handler.upload_file(tmp_metadata_path, bucket, metadata_output_key)

            finally:
                # Cleanup
                if os.path.exists(tmp_raw_path):
                    os.remove(tmp_raw_path)
                if os.path.exists(tmp_metadata_path):
                    os.remove(tmp_metadata_path)

            # Generate presigned URLs
            raw_file_url = s3_handler.generate_presigned_url(bucket, raw_output_key)
            json_file_url = s3_handler.generate_presigned_url(bucket, metadata_output_key)

            # Update database
            db_handler.update_files_status(upload_id, 'raw_file_generated')
            db_handler.update_status_history(status_id, 'raw_file_generated', 'Raw File has been extracted.')
            db_handler.insert_file_conversion(conversion_id, upload_id, 'pending', raw_output_key, raw_file_url, json_file_url)

            # Send success notification
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
            sns_handler.send_success_notification(sns_message)

        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed successfully')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
