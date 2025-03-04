import json
from uuid import uuid4
from notifications import send_error_notification, send_success_notification
from database import update_file_record, insert_status_history, insert_hierarchy_data, insert_patient_data
from zip_processer import process_zip_file_streaming, remove_leftover_zip_files
from series_extract import process_dicom_files, cleanup_extracted_files, extract_patient_data_from_dicom 
import os

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
        if not (object_key.endswith('.zip') or object_key.endswith('/')):
            return {
                "statusCode": 200,
                "body": "File skipped - not for processing"
            }

        # Define temp file paths
        extracted_folder = "/tmp/extracted_dicom"

        # Process ZIP or folder using zip_processor
        extracted_prefix = f"{company_id}/{user_id}/uploads/{upload_id}/extracted/{upload_id}"
        if not process_zip_file_streaming(bucket_name, object_key, extracted_prefix, user_id, company_id, upload_id):
            print("Processing failed")
            return {
                "statusCode": 500,
                "body": "Processing failed"
            }

        # Check the contents of the extracted folder
        if os.path.exists(extracted_folder):
            print(f"Contents of extracted folder: {os.listdir(extracted_folder)}")
        else:
            print(f"Directory does not exist: {extracted_folder}")

        remove_leftover_zip_files(extracted_folder)

        # Process DICOM files
        hierarchy = process_dicom_files(extracted_folder,user_id,upload_id)
        # Print the hierarchy to see if it's populated
        # Cleanup extracted files
        print(f"Skipping duplicate entry for series_id: {hierarchy}")
        if not insert_hierarchy_data(hierarchy):
            send_error_notification(
                error_type="DATABASE_ERROR",
                error_title="Hierarchy Insertion Failed",
                error_description="Failed to insert hierarchy data into the database.",
                user_id=user_id
            )

        patients = extract_patient_data_from_dicom(extracted_folder,user_id,upload_id)
        print(f"Checking entries for Patients: {patients}")
        if not insert_patient_data(patients):
            send_error_notification(
                error_type="DATABASE_ERROR",
                error_title="patients Insertion Failed",
                error_description="Failed to insert patients data into the database.",
                user_id=user_id
            )
        cleanup_extracted_files(extracted_folder)

        # Update file record status
        if not update_file_record(upload_id, "extracted"):
            send_error_notification(
                error_type="DATABASE_ERROR",
                error_title="Status Update Failed",
                error_description=f"Failed to update status to EXTRACTED for upload ID: {upload_id}",
                user_id=user_id
            )

        # Insert status history
        if not insert_status_history(upload_id, user_id, "extracted", status_id, "Extraction complete"):
            send_error_notification(
                error_type="DATABASE_ERROR",
                error_title="Status History Update Failed",
                error_description=f"Failed to insert EXTRACTED status history for upload ID: {upload_id}",
                user_id=user_id
            )

        # Notify next step
        send_success_notification(company_id, user_id, status_id, upload_id, extracted_prefix, bucket_name)

        return {
            "statusCode": 200,
            "body": json.dumps(hierarchy, default=str)
        }

    except Exception as e:
        error_description = f"Unexpected error during file processing: {str(e)}"
        print(error_description)  # Log the error for debugging
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
