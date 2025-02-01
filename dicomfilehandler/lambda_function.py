import json
from uuid import uuid4
from notifications import send_error_notification, send_success_notification
from database import update_file_record, insert_status_history
from zip_processor import process_zip_file_streaming

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
