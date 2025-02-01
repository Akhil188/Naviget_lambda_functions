import json
from clients import sns_client, ERROR_TOPIC_ARN, SNS_TOPIC_ARN
from datetime import datetime

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

def send_success_notification(company_id: str, user_id: str, status_id: str, upload_id: str, extracted_prefix: str, bucket_name: str) -> None:
    """Send success notification via SNS"""
    if SNS_TOPIC_ARN:
        message_content = {
            "company_id": company_id,
            "user_id": user_id,
            "status_id": status_id,
            "upload_id": upload_id,
            "status": "Success",
            "message": f"Successfully extracted and uploaded files from {bucket_name}/{extracted_prefix}",
            "timestamp": datetime.utcnow().isoformat()
        }

        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message_content)
        )
        print(f"SNS message published successfully: {json.dumps(message_content)}")
        print(f"Published SNS Message. Message ID: {sns_response['MessageId']}") 