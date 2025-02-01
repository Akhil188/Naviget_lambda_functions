import boto3
import json
from datetime import datetime
import os

class SNSMessages:
    def __init__(self):
        self.client = boto3.client('sns')
        self.sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
        self.error_topic_arn = os.getenv('ERROR_TOPIC_ARN')

    def send_error_notification(self, error_type: str, error_title: str, error_description: str, user_id: str) -> None:
        """Send error notification via SNS."""
        try:
            message = {
                'error_type': error_type,
                'error_title': error_title,
                'error_description': error_description,
                'user_id': user_id
            }
            
            self.client.publish(
                TopicArn=self.error_topic_arn,
                Message=json.dumps(message)
            )
            print(f"Error notification sent: {error_type} - {error_title}")
            
        except Exception as e:
            print(f"Failed to send error notification: {str(e)}")

    def send_completion_notification(self, company_id: str, user_id: str, upload_id: str, conversion_id: str) -> str:
        """Send completion notification via SNS."""
        completion_message = {
            "company_id": company_id,
            "user_id": user_id,
            "upload_id": upload_id,
            "conversion_id": conversion_id,
            "status": "Completed",
            "timestamp": datetime.utcnow().isoformat(),
        }

        response = self.client.publish(
            TopicArn=self.sns_topic_arn,
            Message=json.dumps(completion_message),
            Subject='File Processing Completed'
        )
        return response['MessageId'] 