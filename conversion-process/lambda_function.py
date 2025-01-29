import json
import boto3
import os
import tempfile
from datetime import datetime
from uuid import uuid4
from supabase import create_client, Client

def validate_uuid(value, field_name):
    if not value or not isinstance(value, str):
        raise ValueError(f"{field_name} is missing or not a string: {value}")
    try:
        uuid.UUID(value)
    except ValueError:
        raise ValueError(f"{field_name} is not a valid UUID: {value}")


# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
ERROR_TOPIC_ARN = os.getenv('ERROR_TOPIC_ARN')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)


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


def update_files_status(file_id, status):
    """Update the status of a file in the files table."""
    data = {
        'status': status,
        'updated_at': datetime.utcnow().isoformat()
    }
    response = supabase.table('files').update(data).eq('file_id', file_id).execute()
    # if response.error:
    #     raise Exception(f"Failed to update file status: {response.error}")

def update_status_history(status_id, status, details):
    data = {
        'status': status,
        'details': details,
    }
    response = supabase.table('status_history').update(data).eq('status_id', status_id).execute()
    # if response.error:
    #     raise Exception(f"Failed to insert status history: {response.error}")

def update_file_conversion(conversion_id,output_file_key, status):
    """Update the conversion status in the file_conversion table."""
    data = {
        'status': status,
        'output_folder':output_file_key, 
        'completed_at': datetime.utcnow().isoformat()
    }
    response = supabase.table('file_conversion').update(data).eq('conversion_id', conversion_id).execute()
    # if response.error:
    #     raise Exception(f"Failed to update file conversion status: {response.error}")
def update_user_status(user_id):
    # Prepare the data to be updated
    data = {
        'status': 'completed',
    }
    # Execute the update query
    response = ( supabase .table('users').update(data).eq('user_id', user_id).execute() )
# def insert_conversion_history(file_id, status, details=None):
#     """Insert a conversion state record in the conversion_history table."""
#     data = {
#         'conversion_id': str(uuid4()),
#         'file_id': file_id,
#         'conversion_state': status,
#         'details': details,
#         'created_at': datetime.utcnow().isoformat()
#     }
#     response = supabase.table('conversion_history').insert(data).execute()
#     if response.error:
#         raise Exception(f"Failed to insert conversion history: {response.error}")

def lambda_handler(event, context):
    print("### Lambda 3 Invoked ###")
    print("Event: ", json.dumps(event, indent=4))

    try:
        # Extract SNS message details
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        print("Parsed SNS Message: ", sns_message)

        company_id = sns_message.get('company_id')
        user_id = sns_message.get('user_id')
        upload_id = sns_message.get('upload_id')
        status_id = sns_message.get('status_id')
        conversion_id = sns_message.get('conversion_id')

        # Define S3 output folder path based on provided structure
        base_output_path = f"{company_id}/{user_id}/uploads/{upload_id}/output/"

        # Simulate processing by generating three output files
        output_files = []
        for idx in range(1, 4):
            output_file_key = f"{base_output_path}{upload_id}-output{idx}"
            
            # Create temporary file with dummy content
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(f"Output file {idx} content for upload ID: {upload_id}".encode())
                tmp_file.flush()

                # Upload to S3
                s3_client.upload_file(tmp_file.name, 'naviget-chandu-test', output_file_key)
                print(f"Uploaded output file: {output_file_key}")

                output_files.append(output_file_key)
            
            os.remove(tmp_file.name)  # Clean up local temp file

        # Update files table to mark processing complete
        update_files_status(upload_id, 'converted')
        # Log status change in status_history
        update_status_history(status_id, 'converted', 'Output files generated.')

        # Update file_conversion table
        update_file_conversion(conversion_id,output_file_key,'completed')
        print(f"Updated file_conversion table with status 'completed' for upload ID {upload_id}")
        update_user_status(user_id)

        # # Log conversion complete in conversion_history
        # insert_conversion_history(upload_id, 'completed', 'Output files created and uploaded.')
        # print(f"Logged conversion complete in conversion_history for upload ID {upload_id}")

        # Publish completion message to SNS
        completion_message = {
            "company_id": company_id,
            "user_id": user_id,
            "upload_id": upload_id,
            "conversion_id":conversion_id,
            "status": "Completed",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"SNS_TOPIC_ARN: {completion_message}")

        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(completion_message),
            Subject='File Processing Completed'
        )
        print(f"Published completion message to SNS: {sns_response['MessageId']}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                'message': 'Output files generated successfully.',
                'upload_id': upload_id,
                'output_files': output_files
            })
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"Error occurred: {str(e)}"
            })
        }
