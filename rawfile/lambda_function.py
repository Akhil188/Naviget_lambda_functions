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
from openai_handler import OpenAIHandler

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
openai_handler = OpenAIHandler()

def is_dicom_file(key: str) -> bool:
    """Check if the file is likely a DICOM file based on extension or lack thereof"""
    lower_key = key.lower()
    non_dicom_extensions = {'.txt', '.py', '.raw', '.json', '.csv', '.md'}
    return not any(lower_key.endswith(ext) for ext in non_dicom_extensions)

def process_pixel_data(dicom_dataset, key: str) -> np.ndarray:
    """Process pixel data with proper handling of signed/unsigned conversion and scaling"""
    try:
        # Handle JPEG compressed images
        if not hasattr(dicom_dataset, 'pixel_array'):
            raise RuntimeError("No pixel data available")
            
        pixel_array = dicom_dataset.pixel_array
        
        # Get bit depth information
        bits_stored = getattr(dicom_dataset, 'BitsStored', 16)
        pixel_representation = getattr(dicom_dataset, 'PixelRepresentation', 0)
        
        # Convert to float32 first to prevent overflow during calculations
        pixel_array = pixel_array.astype(np.float32)
        
        # Scale down values if they exceed int16 range
        max_val = np.max(pixel_array)
        if max_val > 32767:
            scale_factor = 32767 / max_val
            pixel_array = pixel_array * scale_factor
            print(f"Scaled pixel data for {key} by factor {scale_factor}")
        
        # Convert to int16 with proper handling of signed/unsigned
        if pixel_representation == 1:  # Signed
            pixel_array = np.clip(pixel_array, -32768, 32767)
        else:  # Unsigned
            pixel_array = np.clip(pixel_array, 0, 32767)
        
        pixel_array = pixel_array.astype(np.int16)
        
        print(f"Successfully processed pixel data for {key}")
        return pixel_array
        
    except Exception as e:
        print(f"Error in process_pixel_data for {key}: {str(e)}")
        raise

def lambda_handler(event, context):
    print("### Lambda Invoked ###")
    conversion_id = str(uuid4())
    
    # Check for required DICOM plugins
    try:
        import pylibjpeg
    except ImportError:
        print("Warning: pylibjpeg not installed. JPEG compressed DICOM files may fail.")
        
    try:
        import openjpeg
    except ImportError:
        print("Warning: openjpeg not installed. JPEG2000 compressed DICOM files may fail.")
    
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

            # Fetch series_ids with active status
            series_ids = db_handler.fetch_series_ids(upload_id)  # Assuming upload_id is used to fetch series_ids
            if not series_ids:
                print(f"No active series found for upload_id: {upload_id}")
                continue  # Skip processing if no active series_ids

            # Initialize series_data to store pixel data and metadata for each series_id
            series_data = {series_id: {'pixel_data': [], 'metadata': {}} for series_id in series_ids}

            # Define S3 paths
            bucket = 'naviget-user-data-files'
            folder_key = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}"
            print("----------------------------")
            print('folder_key', folder_key)
            folder_key = folder_key.rstrip('/') + '/'
            print(f"S3 Folder Key: {folder_key}")

            # List all objects inside the extracted folder
            response = s3_handler.list_objects(bucket, folder_key)
            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith("/"):
                        continue
                    # Dynamically determine the deepest subfolder where files exist
                    possible_folder = os.path.dirname(obj["Key"]) + "/"
                    print(f"Detected actual folder path: {possible_folder}")
                    folder_key = possible_folder  # Update to the correct folder

            # Fetch files from the dynamically detected folder
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
                continue  # Skip to the next series_id

            # Process DICOM files for the current series_id
            combined_pixel_data = []
            combined_metadata = {}
            valid_shape = (512, 512)
            processed_files = 0
            warning_messages = []

            for file_obj in response['Contents']:
                key = file_obj['Key']
                print(f"Processing file: {key}")  # Log the file being processed
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
                        series_instance_uid = getattr(dicom_dataset, 'SeriesInstanceUID', None)
                        print(f"File Series ID: {series_instance_uid}")

                    if series_instance_uid in series_data:
                        if hasattr(dicom_dataset, "PixelData"):
                            try:
                                pixel_array = process_pixel_data(dicom_dataset, key)
                                current_shape = pixel_array.shape
                                
                                if current_shape == valid_shape:
                                    series_data[series_instance_uid]['pixel_data'].append(pixel_array)
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
                        series_data[series_instance_uid]['metadata'].update(file_metadata)
                except Exception as e:
                    print(f"Error processing DICOM file {key}: {str(e)}")
                    warning_messages.append(f"Failed to process {os.path.basename(key)}: {str(e)}")
                finally:
                    os.remove(tmp_input_path)

            # Process combined data for each series in series_ids
            for series_id in series_ids:
                if not series_data[series_id]['pixel_data']:
                    print(f"No valid data for Series ID: {series_id}")
                    continue

                try:
                    combined_pixel_data = np.concatenate(series_data[series_id]['pixel_data'], axis=0)
                    print(f"Successfully combined files for Series {series_id} into shape {combined_pixel_data.shape}")
                except Exception as e:
                    error_msg = f"Error combining pixel data for Series {series_id}: {str(e)}"
                    print(error_msg)
                    sns_handler.send_error_notification(
                        error_type="PROCESSING_ERROR",
                        error_title="Data Combination Failed",
                        error_description=error_msg,
                        user_id=user_id
                    )
                    continue

                # Define output paths
                parent_folder = f"{company_id}/{user_id}/uploads/{upload_id}/temp"
                raw_output_key = f"{parent_folder}/Raw_files/{series_id}.raw"
                metadata_output_key = f"{parent_folder}/Json_files/{series_id}-settings.json"
                
                tmp_raw_path = f"/tmp/{series_id}.raw"
                tmp_metadata_path = f"/tmp/{series_id}-settings.json"
                
                # Save raw data
                combined_pixel_data.tofile(tmp_raw_path)
                s3_handler.upload_file(tmp_raw_path, bucket, raw_output_key)

                # Save metadata
                with open(tmp_metadata_path, 'w') as meta_file:
                    json.dump(series_data[series_id]['metadata'], meta_file)
                s3_handler.upload_file(tmp_metadata_path, bucket, metadata_output_key)

                # Generate presigned URLs
                raw_file_url = s3_handler.generate_presigned_url(bucket, raw_output_key)
                json_file_url = s3_handler.generate_presigned_url(bucket, metadata_output_key)

                # After processing combined data
                # Get study description from metadata
                study_description = series_data[series_id]['metadata'].get('StudyDescription', '')
                image_url = None  # Initialize image_url

                if study_description:
                    # Generate and save the AI image
                    image_data = openai_handler.generate_medical_image(study_description)
                    if image_data:
                        # Define image path
                        image_output_key = f"{parent_folder}/images/{upload_id}_{series_id}_ai_visualization.png"  # Store in images folder
                        tmp_image_path = f"/tmp/{upload_id}_{series_id}_ai_visualization.png"
                        
                        # Save image temporarily
                        with open(tmp_image_path, 'wb') as f:
                            f.write(image_data)
                        
                        # Upload to S3
                        s3_handler.upload_file(tmp_image_path, bucket, image_output_key)
                        
                        # Generate presigned URL for the image
                        image_url = s3_handler.generate_presigned_url(bucket, image_output_key)
                        
                        # Cleanup
                        os.remove(tmp_image_path)

                # Update database with presigned URLs
                db_handler.insert_file_conversion(conversion_id, upload_id, 'pending', raw_output_key, raw_file_url, json_file_url, image_url)

                # Send success notification
                sns_message = {
                    "company_id": company_id,
                    "user_id": user_id,
                    "upload_id": upload_id,
                    "status_id": status_id,
                    "conversion_id": conversion_id,
                    "status": "Success",
                    "message": "Successfully extracted raw file and generated AI image",
                    "timestamp": datetime.utcnow().isoformat()
                }
                print(sns_message)
                sns_handler.send_success_notification(sns_message)

            # Ensure cleanup is outside the for loop
            # finally:
            #     # Cleanup
            #     if os.path.exists(tmp_raw_path):
            #         os.remove(tmp_raw_path)
            #     if os.path.exists(tmp_metadata_path):
            #         os.remove(tmp_metadata_path)

        return {
            'statusCode': 200,
            'body': json.dumps("Processing completed successfully")
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
